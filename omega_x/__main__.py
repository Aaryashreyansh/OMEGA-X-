import argparse
import asyncio
import json
from .engine import demo

def main():
    parser = argparse.ArgumentParser(description="Run the OMEGA-X TITAN task graph")
    parser.add_argument("--state-dir", default=".omega-state",
                        help="Persistent journal/CAS directory (default: .omega-state)")
    args = parser.parse_args()
    report = asyncio.run(demo(state_dir=args.state_dir))
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
