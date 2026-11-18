import asyncio
import json
from .engine import demo

if __name__ == "__main__":
    print(json.dumps(asyncio.run(demo()), indent=2))
