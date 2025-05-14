import tritonclient.http as httpclient
import numpy as np


class TritonClient:
    def __init__(self, url: str = None):
        self.client = httpclient.InferenceServerClient(
            url=url or "triton:8000"
        )

    def preprocess(self, data: dict) -> np.ndarray:
        features = data.get("input")
        if not isinstance(features, list):
            raise ValueError("❌ Triton 입력 형식이 올바르지 않습니다.")

        input_array = np.array(features, dtype=np.float32)
        if input_array.ndim == 2:
            input_array = np.expand_dims(input_array, axis=0)  # [1, seq_len, dim]
        return input_array

    def postprocess(self, output: np.ndarray) -> list:
        return output.tolist()

    def infer(self, model_name: str, data: dict) -> list:
        input_array = self.preprocess(data)
        inputs = [httpclient.InferInput("INPUT", input_array.shape, "FP32")]
        inputs[0].set_data_from_numpy(input_array)

        outputs = [httpclient.InferRequestedOutput("OUTPUT")]
        result = self.client.infer(model_name, inputs=inputs, outputs=outputs)

        return self.postprocess(result.as_numpy("OUTPUT"))

    def is_model_ready(self, model_name: str) -> bool:
        return self.client.is_model_ready(model_name)

    def get_model_metadata(self, model_name: str) -> dict:
        return self.client.get_model_metadata(model_name)

    def get_model_config(self, model_name: str) -> dict:
        return self.client.get_model_config(model_name)

    def get_inference_statistics(self, model_name: str) -> dict:
        return self.client.get_inference_statistics(model_name)

    def infer_signal(self, data: dict, models: list[str]) -> dict:
        """
        여러 모델 추론 결과를 하나의 dict로 조합하여 signal 후보를 반환.
        예: {volume_ae: 0.03, trend_segmenter: [0.1, 0.9], risk_scorer: 0.72}
        """
        signal = {}
        for model_name in models:
            try:
                output = self.infer(model_name, data)
                if len(output) == 1 and isinstance(output[0], list):
                    signal[model_name] = output[0]
                elif isinstance(output, list):
                    signal[model_name] = output
                else:
                    signal[model_name] = [output]
            except Exception as e:
                print(f"❌ 모델 '{model_name}' 추론 실패: {e}")
                signal[model_name] = None
        return signal
