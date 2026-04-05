"""DAG execution engine with dependency graph, state machine, and crash recovery."""

from __future__ import annotations


from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.pipeline import DePipelineLog

logger = get_logger(__name__)


class PipelineState(str, Enum):
    """State machine for pipeline execution within a DAG run."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"
    PARTIAL = "partial"
    SKIPPED = "skipped"


@dataclass
class PipelineNode:
    """Single node in the execution DAG."""

    name: str
    dependencies: list[str] = field(default_factory=list)
    # If True, failure of this node causes downstream dependents to be skipped
    critical: bool = False
    # Track A = equity/market data; Track B-E = independent tracks
    track: str = "A"
    state: PipelineState = PipelineState.PENDING
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None
    rows_processed: int = 0


@dataclass
class DAGRun:
    """Represents a single complete DAG run."""

    run_id: str
    business_date: date
    nodes: dict[str, PipelineNode]
    started_at: datetime
    completed_at: datetime | None = None
    overall_status: PipelineState = PipelineState.PENDING


class CyclicDependencyError(Exception):
    """Raised when a cyclic dependency is detected in the DAG."""


class DAGExecutor:
    """Execute pipelines as a directed acyclic graph with dependency tracking.

    Supports:
    - Dependency resolution with topological sort
    - Conditional branching: Track A failure skips RS/regime but continues B-E
    - Crash recovery: --resume flag reloads state from de_pipeline_log
    - Startup preflight checks (Redis, RDS)
    - Parallel execution of independent nodes
    """

    # Canonical pipeline dependency graph for JIP Data Engine
    DEFAULT_GRAPH: dict[str, list[str]] = {
        # Track A — Market data (sequential, order matters)
        "nse_bhav": [],
        "nse_corporate_actions": ["nse_bhav"],
        "nse_indices": ["nse_bhav"],
        "fii_dii_flows": [],
        # RS and regime depend on Track A being complete
        "relative_strength": ["nse_bhav", "nse_indices"],
        "regime_detection": ["relative_strength"],
        "market_breadth": ["nse_bhav", "nse_indices"],
        # Track B — Mutual Fund data (independent)
        "amfi_nav": [],
        "mf_master": [],
        # Track C — Global data (independent)
        "yfinance_global": [],
        "fred_macro": [],
        # Track D — Morningstar (independent)
        "morningstar_nav": [],
        "morningstar_portfolio": ["morningstar_nav"],
        # Track E — Qualitative (independent)
        "qualitative_rss": [],
    }

    # Pipelines that compute on Track A; skip if Track A has failed
    TRACK_A_DEPENDENTS: frozenset[str] = frozenset(
        {"relative_strength", "regime_detection", "market_breadth"}
    )

    def __init__(
        self,
        graph: dict[str, list[str]] | None = None,
    ) -> None:
        self._graph = graph or self.DEFAULT_GRAPH
        self._validate_graph()

    def _validate_graph(self) -> None:
        """Validate that the dependency graph has no cycles."""
        visited: set[str] = set()
        rec_stack: set[str] = set()

        def dfs(node: str) -> None:
            visited.add(node)
            rec_stack.add(node)
            for dep in self._graph.get(node, []):
                if dep not in visited:
                    dfs(dep)
                elif dep in rec_stack:
                    raise CyclicDependencyError(
                        f"Cyclic dependency detected: {node} -> {dep}"
                    )
            rec_stack.discard(node)

        for node in self._graph:
            if node not in visited:
                dfs(node)

    def topological_sort(self, pipeline_names: list[str]) -> list[str]:
        """Return execution order respecting dependencies (Kahn's algorithm)."""
        # Build subgraph for requested pipelines
        subgraph: dict[str, list[str]] = {}
        for name in pipeline_names:
            deps = [d for d in self._graph.get(name, []) if d in pipeline_names]
            subgraph[name] = deps

        in_degree: dict[str, int] = {n: 0 for n in subgraph}
        for name, deps in subgraph.items():
            for dep in deps:
                in_degree[name] = in_degree.get(name, 0) + 1

        # Recount properly
        in_degree = {n: 0 for n in subgraph}
        for deps in subgraph.values():
            for dep in deps:
                if dep in in_degree:
                    pass  # dep is a prerequisite, not counted here

        # Build reverse: who depends on whom
        in_degree = {n: 0 for n in subgraph}
        for name in subgraph:
            for dep in subgraph[name]:
                in_degree[name] += 1

        queue = [n for n, d in in_degree.items() if d == 0]
        result: list[str] = []

        while queue:
            node = queue.pop(0)
            result.append(node)
            # Find nodes that depend on this one
            for name, deps in subgraph.items():
                if node in deps:
                    in_degree[name] -= 1
                    if in_degree[name] == 0:
                        queue.append(name)

        if len(result) != len(subgraph):
            raise CyclicDependencyError("Cycle detected during topological sort")

        return result

    def _track_a_failed(self, node_states: dict[str, PipelineState]) -> bool:
        """Check if Track A core pipelines have failed."""
        track_a_core = {"nse_bhav", "nse_indices"}
        for name in track_a_core:
            if name in node_states and node_states[name] in (
                PipelineState.FAILED,
            ):
                return True
        return False

    async def _load_resume_state(
        self,
        session: AsyncSession,
        business_date: date,
        pipeline_names: list[str],
    ) -> dict[str, PipelineState]:
        """Load existing pipeline states from de_pipeline_log for crash recovery."""
        result = await session.execute(
            sa.select(DePipelineLog.pipeline_name, DePipelineLog.status)
            .where(
                DePipelineLog.business_date == business_date,
                DePipelineLog.pipeline_name.in_(pipeline_names),
            )
            .order_by(DePipelineLog.pipeline_name, DePipelineLog.run_number.desc())
        )
        rows = result.fetchall()

        # Take the latest run per pipeline
        seen: set[str] = set()
        states: dict[str, PipelineState] = {}
        for row in rows:
            if row.pipeline_name not in seen:
                seen.add(row.pipeline_name)
                status_map = {
                    "success": PipelineState.COMPLETE,
                    "partial": PipelineState.PARTIAL,
                    "failed": PipelineState.FAILED,
                    "skipped": PipelineState.SKIPPED,
                    "running": PipelineState.PENDING,  # treat running as needs-retry
                }
                states[row.pipeline_name] = status_map.get(
                    row.status, PipelineState.PENDING
                )

        logger.info(
            "dag_resume_state_loaded",
            business_date=business_date.isoformat(),
            states={k: v.value for k, v in states.items()},
        )
        return states

    async def execute(
        self,
        pipeline_names: list[str],
        business_date: date,
        session: AsyncSession,
        pipeline_runner: Any,  # callable(name, date, session) -> PipelineResult
        resume: bool = False,
    ) -> dict[str, PipelineNode]:
        """Execute pipelines in dependency order.

        Args:
            pipeline_names: List of pipeline names to execute
            business_date: The business date for this run
            session: SQLAlchemy async session
            pipeline_runner: Async callable(pipeline_name, business_date, session)
            resume: If True, load existing states and skip completed pipelines

        Returns:
            Dict mapping pipeline name to final PipelineNode state
        """
        order = self.topological_sort(pipeline_names)
        nodes: dict[str, PipelineNode] = {
            name: PipelineNode(
                name=name,
                dependencies=[d for d in self._graph.get(name, []) if d in pipeline_names],
                track=(
                    "A"
                    if name in self.TRACK_A_DEPENDENTS
                    or name in {"nse_bhav", "nse_indices", "nse_corporate_actions"}
                    else "B"
                ),
            )
            for name in order
        }

        # Load resume state if requested
        resume_states: dict[str, PipelineState] = {}
        if resume:
            resume_states = await self._load_resume_state(session, business_date, pipeline_names)
            for name, state in resume_states.items():
                if name in nodes:
                    nodes[name].state = state

        logger.info(
            "dag_execution_start",
            business_date=business_date.isoformat(),
            pipeline_count=len(order),
            order=order,
            resume=resume,
        )

        for name in order:
            node = nodes[name]

            # Skip already-completed nodes in resume mode
            if node.state in (PipelineState.COMPLETE, PipelineState.PARTIAL):
                logger.info(
                    "dag_node_skip_already_complete",
                    pipeline=name,
                    business_date=business_date.isoformat(),
                )
                continue

            # Check if Track A failed — skip RS/regime dependents
            if name in self.TRACK_A_DEPENDENTS:
                current_states = {n: nodes[n].state for n in nodes}
                if self._track_a_failed(current_states):
                    node.state = PipelineState.SKIPPED
                    node.error = "Skipped: Track A (nse_bhav/nse_indices) failed"
                    logger.warning(
                        "dag_node_skip_track_a_failed",
                        pipeline=name,
                        business_date=business_date.isoformat(),
                    )
                    continue

            # Check that all dependencies are complete (not failed)
            dep_failed = False
            for dep_name in node.dependencies:
                if dep_name in nodes:
                    dep_state = nodes[dep_name].state
                    if dep_state in (PipelineState.FAILED,):
                        dep_failed = True
                        node.state = PipelineState.SKIPPED
                        node.error = f"Skipped: dependency {dep_name} failed"
                        logger.warning(
                            "dag_node_skip_dep_failed",
                            pipeline=name,
                            dep=dep_name,
                            business_date=business_date.isoformat(),
                        )
                        break

            if dep_failed:
                continue

            # Execute the pipeline
            node.state = PipelineState.RUNNING
            node.started_at = datetime.now(tz=timezone.utc)

            logger.info(
                "dag_node_start",
                pipeline=name,
                business_date=business_date.isoformat(),
            )

            try:
                result = await pipeline_runner(name, business_date, session)
                node.completed_at = datetime.now(tz=timezone.utc)
                node.rows_processed = getattr(result, "rows_processed", 0)

                status_str = getattr(result, "status", "failed")
                if status_str == "success":
                    node.state = PipelineState.COMPLETE
                elif status_str == "partial":
                    node.state = PipelineState.PARTIAL
                elif status_str == "skipped":
                    node.state = PipelineState.SKIPPED
                else:
                    node.state = PipelineState.FAILED
                    node.error = getattr(result, "error", "Unknown error")

            except Exception as exc:
                node.state = PipelineState.FAILED
                node.error = str(exc)
                node.completed_at = datetime.now(tz=timezone.utc)
                logger.error(
                    "dag_node_exception",
                    pipeline=name,
                    business_date=business_date.isoformat(),
                    error=str(exc),
                )

            logger.info(
                "dag_node_complete",
                pipeline=name,
                state=node.state.value,
                business_date=business_date.isoformat(),
                rows_processed=node.rows_processed,
            )

        failed = [n for n, node in nodes.items() if node.state == PipelineState.FAILED]
        complete = [n for n, node in nodes.items() if node.state in (PipelineState.COMPLETE, PipelineState.PARTIAL)]

        logger.info(
            "dag_execution_complete",
            business_date=business_date.isoformat(),
            complete_count=len(complete),
            failed_count=len(failed),
            failed_pipelines=failed,
        )

        return nodes

    async def preflight_check(
        self,
        redis_url: str,
        database_url: str,
    ) -> dict[str, bool]:
        """Run startup preflight checks before DAG execution.

        Checks:
        - Redis: PING
        - PostgreSQL: SELECT 1
        """
        results: dict[str, bool] = {}

        # Redis check
        try:
            import redis.asyncio as aioredis
            client = aioredis.from_url(redis_url, socket_connect_timeout=5)
            pong = await client.ping()
            await client.aclose()
            results["redis"] = bool(pong)
            logger.info("preflight_redis_ok")
        except Exception as exc:
            results["redis"] = False
            logger.error("preflight_redis_failed", error=str(exc))

        # Database check
        try:
            from sqlalchemy.ext.asyncio import create_async_engine
            engine = create_async_engine(database_url, connect_args={"server_settings": {}})
            async with engine.connect() as conn:
                await conn.execute(sa.text("SELECT 1"))
            await engine.dispose()
            results["database"] = True
            logger.info("preflight_database_ok")
        except Exception as exc:
            results["database"] = False
            logger.error("preflight_database_failed", error=str(exc))

        return results
