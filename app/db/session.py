from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy import create_engine
from app.config import get_settings

settings = get_settings()

# Async engine for FastAPI endpoints
async_engine = create_async_engine(
    settings.database_url,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    echo=False,
)

# Async session factory
async_session_factory = async_sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

# Sync engine for Alembic migrations
sync_engine = create_engine(
    settings.database_url_sync,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    echo=False,
)


async def get_db() -> AsyncSession:
    """Dependency for FastAPI endpoints."""
    async with async_session_factory() as session:
        async with session.begin():
            yield session
