import asyncio
import asyncpg


async def main():
    conn = await asyncpg.connect(
        "postgresql://postgres:apex@localhost:5433/apex_ledger"
    )
    print("CONNECTED")
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
