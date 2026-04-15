"""
智谱 AI 模型调用工具类（使用 zhipuai SDK）

支持:
- glm-4-flash: 文本生成与对话
- glm-4.6v-flash: 图像理解
"""

import base64
import io
from typing import Optional, List, Dict, Any, Union
from PIL import Image
from zhipuai import ZhipuAI


class ZhipuAIModel:
    """智谱 AI 模型调用工具类"""

    # 模型常量
    MODEL_TEXT = "glm-4-flash"
    MODEL_IMAGE = "glm-4.6v-flash"

    def __init__(self, api_key: str, max_retries: int = 0):
        """
        初始化智谱 AI 客户端

        Args:
            api_key: 智谱 AI API Key
        """
        self.client = ZhipuAI(api_key=api_key, max_retries=max_retries)

    def text_chat(
        self,
        prompt: str,
        messages: Optional[List[Dict[str, str]]] = None,
        temperature: float = 0.7,
        max_tokens: int = 2048,
        thinking: bool = False
    ) -> Dict[str, Any]:
        """
        文本对话/生成

        Args:
            prompt: 用户输入内容
            messages: 对话历史列表，格式 [{"role": "user/assistant", "content": "..."}]
            temperature: 温度参数 (0.0-1.0)，默认 0.7
            max_tokens: 最大生成 token 数，默认 2048
            thinking: 是否启用深度思考模式（COT），默认 False

        Returns:
            Dict: 包含 content, reasoning_content, usage 等信息
        """
        # 构建 messages
        if messages is None:
            messages = [{"role": "user", "content": prompt}]
        elif prompt:
            messages = messages + [{"role": "user", "content": prompt}]

        # 构建请求参数
        kwargs = {
            "model": self.MODEL_TEXT,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens
        }

        # 启用深度思考模式
        if thinking:
            kwargs["thinking"] = {"type": "enabled"}

        # 调用 API
        response = self.client.chat.completions.create(**kwargs)

        # 解析结果
        choice = response.choices[0]
        message = choice.message
        content = getattr(message, "content", "") if not isinstance(message, dict) else message.get("content", "")
        reasoning_content = getattr(message, "reasoning_content", "") if not isinstance(message, dict) else message.get("reasoning_content", "")

        return {
            "content": content or "",
            "reasoning_content": reasoning_content or "",
            "usage": response.usage if hasattr(response, 'usage') else None,
            "raw_response": response
        }

    def image_understand(
        self,
        image: Union[str, Image.Image, bytes],
        prompt: str = "请详细描述这张图片的内容",
        temperature: float = 0.7,
        max_tokens: int = 2048
    ) -> Dict[str, Any]:
        """
        图像理解

        Args:
            image: 图片，支持以下格式:
                - str: 图片文件路径
                - PIL.Image.Image: PIL Image 对象
                - bytes: 图片二进制数据
            prompt: 提示词，默认 "请详细描述这张图片的内容"
            temperature: 温度参数，默认 0.7
            max_tokens: 最大生成 token 数，默认 2048

        Returns:
            Dict: 包含 content 和 usage 信息
        """
        # 处理图片输入
        image_base64 = self._encode_image(image)

        # 调用 API
        response = self.client.chat.completions.create(
            model=self.MODEL_IMAGE,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": image_base64  # SDK 会自动处理 base64
                            }
                        },
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                }
            ],
            temperature=temperature,
            max_tokens=max_tokens
        )

        return {
            "content": response.choices[0].message.content,
            "usage": response.usage if hasattr(response, 'usage') else None,
            "raw_response": response
        }

    def _encode_image(self, image: Union[str, Image.Image, bytes]) -> str:
        """
        将图片编码为 base64 字符串（纯 base64，无前缀）

        Args:
            image: 图片（路径、PIL 对象或二进制数据）

        Returns:
            str: 纯 base64 编码的图片字符串
        """
        # 如果是文件路径，打开图片
        if isinstance(image, str):
            img = Image.open(image)
            return self._image_to_base64(img)

        # 如果是 PIL Image 对象
        elif isinstance(image, Image.Image):
            return self._image_to_base64(image)

        # 如果是二进制数据
        elif isinstance(image, bytes):
            return base64.b64encode(image).decode('utf-8')

        else:
            raise ValueError(f"不支持的图片类型: {type(image)}")

    def _image_to_base64(self, image: Image.Image) -> str:
        """将 PIL Image 转换为纯 base64 字符串（无前缀）"""
        img_byte_arr = io.BytesIO()
        # 保存为 JPEG 或 PNG 格式
        format_type = image.format if image.format in ['JPEG', 'PNG'] else 'PNG'

        image.save(img_byte_arr, format=format_type)
        img_byte_arr = img_byte_arr.getvalue()
        return base64.b64encode(img_byte_arr).decode('utf-8')


# 便捷函数
def create_zhipu_client(api_key: str, max_retries: int = 0) -> ZhipuAIModel:
    """
    创建智谱 AI 客户端实例

    Args:
        api_key: 智谱 AI API Key

    Returns:
        ZhipuAIModel: 客户端实例
    """
    return ZhipuAIModel(api_key, max_retries=max_retries)


# 使用示例
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()

    # 获取 API Key
    api_key = os.getenv("ZHIPUAI_API_KEY")
    if not api_key:
        print("请设置环境变量 ZHIPUAI_API_KEY")
        exit(1)

    # 创建客户端
    client = create_zhipu_client(api_key)

    # 示例 1: 文本对话
    print("=== 文本对话示例 ===")
    result = client.text_chat(
        prompt="用一句话介绍 Python",
        temperature=0.7
    )
    print(f"回答: {result['content']}")
    print(f"Token 使用: {result['usage']}\n")

    # 示例 2: 深度思考模式
    print("=== 深度思考模式示例 ===")
    result = client.text_chat(
        prompt="为什么 1+1=2？请详细推理",
        thinking=True
    )
    if result['reasoning_content']:
        print(f"推理过程: {result['reasoning_content']}")
    print(f"最终答案: {result['content']}\n")

    # 示例 3: 图像理解（需要图片文件）
    print("=== 图像理解示例 ===")
    # result = client.image_understand(
    #     image="path/to/image.jpg",
    #     prompt="这张图片里有什么？"
    # )
    # print(f"分析结果: {result['content']}")
