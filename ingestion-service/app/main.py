import asyncio
from app.services.ingestion import run_ingestion

def main()-> None:
    try:
        asyncio.run(run_ingestion())
    except KeyboardInterrupt:
        print("\n ıngestion service durdu")

if __name__ == "__main__":
    main()