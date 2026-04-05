"""Tests for DAG execution engine."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.orchestrator.dag import (
    CyclicDependencyError,
    DAGExecutor,
    PipelineNode,
    PipelineState,
)


# ---------------------------------------------------------------------------
# Topological sort
# ---------------------------------------------------------------------------


class TestTopologicalSort:
    def test_no_dependencies_returns_all(self) -> None:
        executor = DAGExecutor(graph={"a": [], "b": [], "c": []})
        result = executor.topological_sort(["a", "b", "c"])
        assert set(result) == {"a", "b", "c"}
        assert len(result) == 3

    def test_linear_chain_respects_order(self) -> None:
        executor = DAGExecutor(graph={"a": [], "b": ["a"], "c": ["b"]})
        result = executor.topological_sort(["a", "b", "c"])
        assert result.index("a") < result.index("b")
        assert result.index("b") < result.index("c")

    def test_diamond_dependency(self) -> None:
        # a -> b, a -> c, b -> d, c -> d
        executor = DAGExecutor(graph={"a": [], "b": ["a"], "c": ["a"], "d": ["b", "c"]})
        result = executor.topological_sort(["a", "b", "c", "d"])
        assert result.index("a") < result.index("b")
        assert result.index("a") < result.index("c")
        assert result.index("b") < result.index("d")
        assert result.index("c") < result.index("d")

    def test_subset_of_pipelines(self) -> None:
        executor = DAGExecutor(graph={"a": [], "b": ["a"], "c": ["b"]})
        result = executor.topological_sort(["a", "b"])
        assert "c" not in result
        assert result.index("a") < result.index("b")

    def test_cyclic_graph_raises(self) -> None:
        with pytest.raises(CyclicDependencyError):
            DAGExecutor(graph={"a": ["b"], "b": ["a"]})

    def test_default_graph_is_valid(self) -> None:
        executor = DAGExecutor()
        all_pipelines = list(DAGExecutor.DEFAULT_GRAPH.keys())
        result = executor.topological_sort(all_pipelines)
        assert len(result) == len(all_pipelines)
        # nse_bhav must come before relative_strength
        assert result.index("nse_bhav") < result.index("relative_strength")
        # relative_strength before regime_detection
        assert result.index("relative_strength") < result.index("regime_detection")


# ---------------------------------------------------------------------------
# Track A failure logic
# ---------------------------------------------------------------------------


class TestTrackAFailure:
    def test_nse_bhav_failed_returns_true(self) -> None:
        executor = DAGExecutor()
        states = {"nse_bhav": PipelineState.FAILED, "nse_indices": PipelineState.COMPLETE}
        assert executor._track_a_failed(states) is True

    def test_nse_indices_failed_returns_true(self) -> None:
        executor = DAGExecutor()
        states = {"nse_bhav": PipelineState.COMPLETE, "nse_indices": PipelineState.FAILED}
        assert executor._track_a_failed(states) is True

    def test_all_complete_returns_false(self) -> None:
        executor = DAGExecutor()
        states = {"nse_bhav": PipelineState.COMPLETE, "nse_indices": PipelineState.COMPLETE}
        assert executor._track_a_failed(states) is False

    def test_empty_states_returns_false(self) -> None:
        executor = DAGExecutor()
        assert executor._track_a_failed({}) is False


# ---------------------------------------------------------------------------
# DAG execution
# ---------------------------------------------------------------------------


def _make_pipeline_result(status: str = "success", rows: int = 100) -> MagicMock:
    r = MagicMock()
    r.status = status
    r.rows_processed = rows
    r.error = None
    return r


class TestDAGExecute:
    @pytest.fixture
    def simple_graph(self) -> DAGExecutor:
        return DAGExecutor(graph={"a": [], "b": ["a"]})

    @pytest.fixture
    def mock_session(self) -> AsyncMock:
        session = AsyncMock()
        # execute returns an object with fetchall()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        session.execute.return_value = mock_result
        return session

    @pytest.mark.asyncio
    async def test_simple_two_step_success(
        self, simple_graph: DAGExecutor, mock_session: AsyncMock
    ) -> None:
        async def runner(name: str, d: date, sess: object) -> MagicMock:
            return _make_pipeline_result("success")

        nodes = await simple_graph.execute(
            pipeline_names=["a", "b"],
            business_date=date(2026, 4, 5),
            session=mock_session,
            pipeline_runner=runner,
        )
        assert nodes["a"].state == PipelineState.COMPLETE
        assert nodes["b"].state == PipelineState.COMPLETE

    @pytest.mark.asyncio
    async def test_dependency_failed_skips_downstream(
        self, simple_graph: DAGExecutor, mock_session: AsyncMock
    ) -> None:
        call_count = {"a": 0, "b": 0}

        async def runner(name: str, d: date, sess: object) -> MagicMock:
            call_count[name] += 1
            if name == "a":
                return _make_pipeline_result("failed")
            return _make_pipeline_result("success")

        nodes = await simple_graph.execute(
            pipeline_names=["a", "b"],
            business_date=date(2026, 4, 5),
            session=mock_session,
            pipeline_runner=runner,
        )
        assert nodes["a"].state == PipelineState.FAILED
        assert nodes["b"].state == PipelineState.SKIPPED
        assert call_count["b"] == 0

    @pytest.mark.asyncio
    async def test_track_a_failure_skips_rs_regime(
        self, mock_session: AsyncMock
    ) -> None:
        graph = {
            "nse_bhav": [],
            "nse_indices": [],
            "relative_strength": ["nse_bhav", "nse_indices"],
            "regime_detection": ["relative_strength"],
        }
        executor = DAGExecutor(graph=graph)
        # Override TRACK_A_DEPENDENTS so relative_strength is detected
        # (it's already in the default set)

        called = []

        async def runner(name: str, d: date, sess: object) -> MagicMock:
            called.append(name)
            if name == "nse_bhav":
                return _make_pipeline_result("failed")
            return _make_pipeline_result("success")

        nodes = await executor.execute(
            pipeline_names=["nse_bhav", "nse_indices", "relative_strength", "regime_detection"],
            business_date=date(2026, 4, 5),
            session=mock_session,
            pipeline_runner=runner,
        )
        assert nodes["nse_bhav"].state == PipelineState.FAILED
        # RS and regime should be skipped due to Track A failure
        assert nodes["relative_strength"].state == PipelineState.SKIPPED
        assert nodes["regime_detection"].state == PipelineState.SKIPPED

    @pytest.mark.asyncio
    async def test_exception_in_runner_marks_failed(
        self, simple_graph: DAGExecutor, mock_session: AsyncMock
    ) -> None:
        async def runner(name: str, d: date, sess: object) -> MagicMock:
            raise RuntimeError("network timeout")

        nodes = await simple_graph.execute(
            pipeline_names=["a"],
            business_date=date(2026, 4, 5),
            session=mock_session,
            pipeline_runner=runner,
        )
        assert nodes["a"].state == PipelineState.FAILED
        assert "network timeout" in (nodes["a"].error or "")

    @pytest.mark.asyncio
    async def test_partial_status_preserved(
        self, simple_graph: DAGExecutor, mock_session: AsyncMock
    ) -> None:
        async def runner(name: str, d: date, sess: object) -> MagicMock:
            return _make_pipeline_result("partial")

        nodes = await simple_graph.execute(
            pipeline_names=["a"],
            business_date=date(2026, 4, 5),
            session=mock_session,
            pipeline_runner=runner,
        )
        assert nodes["a"].state == PipelineState.PARTIAL

    @pytest.mark.asyncio
    async def test_resume_skips_completed_pipelines(
        self, simple_graph: DAGExecutor, mock_session: AsyncMock
    ) -> None:
        """With resume=True, completed pipelines are not re-executed."""

        # Mock: "a" is already complete in de_pipeline_log
        mock_row = MagicMock()
        mock_row.pipeline_name = "a"
        mock_row.status = "success"
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [mock_row]
        mock_session.execute.return_value = mock_result

        called = []

        async def runner(name: str, d: date, sess: object) -> MagicMock:
            called.append(name)
            return _make_pipeline_result("success")

        nodes = await simple_graph.execute(
            pipeline_names=["a", "b"],
            business_date=date(2026, 4, 5),
            session=mock_session,
            pipeline_runner=runner,
            resume=True,
        )
        # "a" was already complete → not called again
        assert "a" not in called
        assert nodes["a"].state == PipelineState.COMPLETE
        # "b" should run
        assert "b" in called


# ---------------------------------------------------------------------------
# Pipeline state machine values
# ---------------------------------------------------------------------------


class TestPipelineState:
    def test_all_states_have_string_values(self) -> None:
        for state in PipelineState:
            assert isinstance(state.value, str)

    def test_state_string_equality(self) -> None:
        assert PipelineState.COMPLETE == "complete"
        assert PipelineState.FAILED == "failed"
        assert PipelineState.PENDING == "pending"


# ---------------------------------------------------------------------------
# Pipeline node dataclass
# ---------------------------------------------------------------------------


class TestPipelineNode:
    def test_defaults(self) -> None:
        node = PipelineNode(name="test_pipeline")
        assert node.state == PipelineState.PENDING
        assert node.dependencies == []
        assert node.rows_processed == 0
        assert node.error is None
        assert node.started_at is None
        assert node.completed_at is None
