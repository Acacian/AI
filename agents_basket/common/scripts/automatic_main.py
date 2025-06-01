import os

base_dir = "agents_basket"

template = '''
if __name__ == "__main__":
    import sys
    import traceback
    from .agent import {class_name}

    if len(sys.argv) != 3:
        print("⚠️ 사용법: python -m {module_path} <config_path> <config_key>", flush=True)
        sys.exit(1)

    try:
        config_path = sys.argv[1]
        config_key = sys.argv[2]
        print(f"🚀 Agent 시작: {{config_path}} (key: {{config_key}})", flush=True)
        agent = {class_name}(config_path, config_key)
        agent.run()
    except Exception as e:
        print(f"❌ 에이전트 실행 중 예외 발생: {{e}}", flush=True)
        traceback.print_exc()
        sys.exit(1)
'''

for subdir in os.listdir(base_dir):
    agent_path = os.path.join(base_dir, subdir, "agent.py")
    if os.path.exists(agent_path):
        with open(agent_path, "r") as f:
            lines = f.read()
        if "__main__" in lines:
            continue 

        class_name = None
        for line in lines.splitlines():
            if "class " in line and "BaseAgent" in line:
                class_name = line.split()[1].split("(")[0]
                break
        if not class_name:
            continue

        module_path = f"{base_dir}.{subdir}.agent".replace("/", ".")
        with open(agent_path, "a") as f:
            f.write(template.format(class_name=class_name, module_path=module_path))
        print(f"✅ {agent_path}에 __main__ 추가 완료")
