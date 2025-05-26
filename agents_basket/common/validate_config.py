import yaml
import sys

def validate_and_sort_config(path):
    with open(path, 'r') as f:
        config = yaml.safe_load(f)

    # 필수 키 확인
    required_keys = ["binance_symbols", "macro_symbols", "intervals"]
    for key in required_keys:
        if key not in config:
            print(f"❌ 누락된 키: {key}")
            return

    # 정렬
    config["binance_symbols"] = sorted(config["binance_symbols"])
    config["macro_symbols"] = sorted(config["macro_symbols"])
    config["intervals"] = sorted(config["intervals"])

    # 저장
    with open(path, 'w') as f:
        yaml.dump(config, f, sort_keys=False)
    print(f"✅ config 정렬 완료: {path}")

if __name__ == "__main__":
    config_path = sys.argv[1] if len(sys.argv) > 1 else "collector/config.yml"
    validate_and_sort_config(config_path)
