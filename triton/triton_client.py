import tritonclient.http as httpclient
import numpy as np

class TritonClient:
    def __init__(self, url: str = "triton:8000"):
        self.client = httpclient.InferenceServerClient(url=url)

    def preprocess(self, data: dict) -> np.ndarray:
        features = data.get("input")
        if not isinstance(features, list):
            raise ValueError("❌ Triton 입력 형식이 올바르지 않습니다.")
        x = np.array(features, dtype=np.float32)

        if x.ndim == 3 and x.shape[1] > 100:
            x = x[:, -100:, :]  # 마지막 100개만 자르기
        elif x.ndim == 2 and x.shape[0] > 100:
            x = x[-100:, :]     # 2D일 경우도 고려
            x = np.expand_dims(x, axis=0)

        if x.ndim == 2:
            x = np.expand_dims(x, axis=0)  # [1, seq_len, dim]
        return x

    def postprocess(self, output: np.ndarray) -> list:
        return output.tolist()

    def infer(self, model_name: str, data: dict) -> list:
        x = self.preprocess(data)
        inputs = [httpclient.InferInput("INPUT", x.shape, "FP32")]
        inputs[0].set_data_from_numpy(x)
        outputs = [httpclient.InferRequestedOutput("OUTPUT")]
        result = self.client.infer(model_name, inputs=inputs, outputs=outputs)
        return self.postprocess(result.as_numpy("OUTPUT"))

    def infer_signal(self, data: dict, models: list[str]) -> dict:
        """여러 모델 추론 후 시그널 조합"""
        signal = {}
        for model_name in models:
            try:
                output = self.infer(model_name, data)
                if isinstance(output, list) and len(output) == 1 and isinstance(output[0], list):
                    signal[model_name] = output[0]
                else:
                    signal[model_name] = output
            except Exception as e:
                print(f"❌ 모델 '{model_name}' 추론 실패: {e}")
                signal[model_name] = None
        return signal

    def is_model_ready(self, model_name: str) -> bool:
        return self.client.is_model_ready(model_name)

    def get_model_metadata(self, model_name: str) -> dict:
        return self.client.get_model_metadata(model_name)

    def get_model_config(self, model_name: str) -> dict:
        return self.client.get_model_config(model_name)

    def get_inference_statistics(self, model_name: str) -> dict:
        return self.client.get_inference_statistics(model_name)
