import os
import shutil
import re

BASE_DIR = "agents_basket"
SYMBOLS = ["btc", "eth", "sol"]
MACRO_SYMBOLS = ["gold", "gspc", "tnx"]
ORDERBOOK_SYMBOLS = ["btc"]  # 추후 확장 가능

TARGET_AGENTS = [
    "volume_ae", "volatility_watcher", "overheat_detector", "pattern_ae",
    "noise_filter", "liquidity_checker", "risk_scorer", "trend_segmenter"
]

MACRO_AGENT = "macro_filter"
ORDERBOOK_AGENT = "orderbook_agent"

def patch_agent_py(agent_dir, symbol):
    path = os.path.join(agent_dir, "agent.py")
    if not os.path.exists(path):
        return

    with open(path, "r") as f:
        content = f.read()

    # model_name_prefix 값 수정
    content = re.sub(
        r'model_name_prefix\s*=\s*["\'](.+?)["\']',
        f'model_name_prefix = "{os.path.basename(agent_dir)}"',
        content
    )

    with open(path, "w") as f:
        f.write(content)

def create_symbol_agents():
    for agent in TARGET_AGENTS:
        base_path = os.path.join(BASE_DIR, agent)
        if not os.path.isdir(base_path):
            print(f"⛔ {base_path} 없음")
            continue

        for sym in SYMBOLS:
            new_name = f"{agent}_{sym}"
            new_path = os.path.join(BASE_DIR, new_name)
            if os.path.exists(new_path):
                continue

            shutil.copytree(base_path, new_path)
            patch_agent_py(new_path, sym)
            print(f"✅ Created: {new_path}")

def create_orderbook_agents():
    base_path = os.path.join(BASE_DIR, ORDERBOOK_AGENT)
    if not os.path.isdir(base_path):
        print(f"⛔ {base_path} 없음")
        return

    for sym in ORDERBOOK_SYMBOLS:
        new_name = f"{ORDERBOOK_AGENT}_{sym}"
        new_path = os.path.join(BASE_DIR, new_name)
        if os.path.exists(new_path):
            continue

        shutil.copytree(base_path, new_path)
        patch_agent_py(new_path, sym)
        print(f"✅ Created: {new_path}")

def create_macro_config_entries():
    # macro agent는 디렉토리 복제 없이 config에서만 분기
    for sym in MACRO_SYMBOLS:
        print(f"📝 macro_filter_{sym} → topic: macro_training_{sym}")

if __name__ == "__main__":
    create_symbol_agents()
    create_orderbook_agents()
    create_macro_config_entries()
