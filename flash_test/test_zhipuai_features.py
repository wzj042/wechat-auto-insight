"""
智谱 AI 功能测试脚本

测试功能:
1. 图片理解
2. 长文本主题聚类分析（支持重叠时间区间）
"""

import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from dotenv import load_dotenv
from zhipuai_tool import create_zhipu_client

load_dotenv()


class MessageAnalyzer:
    """消息分析器 - 主题聚类分析"""

    def __init__(self, client):
        self.client = client

    def load_messages(self, json_path: str) -> List[Dict]:
        """加载消息数据"""
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def preprocess_messages(self, messages: List[Dict]) -> List[Dict]:
        """预处理消息 - 过滤有效文本消息"""
        valid_messages = []
        for msg in messages:
            # 只保留文本类型的消息，且文本不为空
            if msg.get('msg_type') == '文本' and msg.get('text'):
                # 过滤掉表情和简单的问候
                text = msg['text'].strip()
                if len(text) > 1 and text not in ['早呀', '早安', '晚安', '哈哈']:
                    valid_messages.append({
                        'time': msg.get('time', ''),
                        'timestamp': msg.get('timestamp', 0),
                        'sender': msg.get('sender', ''),
                        'text': text
                    })
        return valid_messages

    def cluster_messages_by_time(self, messages: List[Dict], window_hours: int = 2) -> List[List[Dict]]:
        """按时间窗口分组消息"""
        if not messages:
            return []

        # 按时间戳排序
        sorted_messages = sorted(messages, key=lambda x: x['timestamp'])

        clusters = []
        current_cluster = [sorted_messages[0]]
        window_start = sorted_messages[0]['timestamp']

        for msg in sorted_messages[1:]:
            # 如果消息时间窗口超过设定值，创建新分组
            if msg['timestamp'] - window_start > window_hours * 3600:
                clusters.append(current_cluster)
                current_cluster = [msg]
                window_start = msg['timestamp']
            else:
                current_cluster.append(msg)

        if current_cluster:
            clusters.append(current_cluster)

        return clusters

    def analyze_topic_for_cluster(self, cluster: List[Dict]) -> Dict[str, Any]:
        """分析单个分组的主题"""
        if not cluster:
            return {}

        # 构建消息摘要
        messages_text = "\n".join([
            f"[{msg['sender']}] {msg['text']}"
            for msg in cluster
        ])

        # 获取时间范围
        timestamps = [msg['timestamp'] for msg in cluster]
        time_range = {
            'start': datetime.fromtimestamp(min(timestamps)).strftime('%Y-%m-%d %H:%M'),
            'end': datetime.fromtimestamp(max(timestamps)).strftime('%Y-%m-%d %H:%M'),
            'message_count': len(cluster)
        }

        # 使用 LLM 分析主题
        prompt = f"""请分析以下聊天消息的主题。

聊天消息（共 {len(cluster)} 条）:
{messages_text}

请以 JSON 格式返回分析结果，包含以下字段：
{{
    "topic": "主题名称（简洁概括）",
    "keywords": ["关键词1", "关键词2", "关键词3"],
    "summary": "简要总结这个时间段讨论的主要内容",
    "participants": ["主要参与者1", "主要参与者2"],
    "sentiment": "情感倾向（positive/neutral/negative）"
}}

注意：
1. 主题应该简洁明确，不超过10个字
2. 关键词提取3-5个最重要的
3. 总结不超过50个字
4. 只返回 JSON，不要其他内容"""

        try:
            result = self.client.text_chat(
                prompt=prompt,
                temperature=0.3
            )

            # 解析 JSON 结果
            content = result['content']
            # 尝试提取 JSON
            import re
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                analysis = json.loads(json_match.group())
            else:
                # 如果无法解析，返回默认值
                analysis = {
                    'topic': '未知主题',
                    'keywords': [],
                    'summary': content[:100],
                    'participants': list(set(msg['sender'] for msg in cluster))[:3],
                    'sentiment': 'neutral'
                }

            return {
                **time_range,
                'topic_analysis': analysis,
                'sample_messages': cluster[:3]  # 保留前3条消息作为示例
            }

        except Exception as e:
            print(f"分析主题时出错: {e}")
            return {
                **time_range,
                'topic_analysis': {
                    'topic': '分析失败',
                    'keywords': [],
                    'summary': str(e),
                    'participants': [],
                    'sentiment': 'neutral'
                }
            }

    def analyze_topics(self, messages: List[Dict], window_hours: int = 2) -> List[Dict]:
        """完整的主题聚类分析流程"""
        print(f"📊 开始分析 {len(messages)} 条消息...")

        # 1. 预处理
        valid_messages = self.preprocess_messages(messages)
        print(f"✅ 过滤后有效消息: {len(valid_messages)} 条")

        # 2. 按时间分组
        clusters = self.cluster_messages_by_time(valid_messages, window_hours)
        print(f"📦 分成 {len(clusters)} 个时间窗口")

        # 3. 分析每个分组的主题
        topics = []
        for i, cluster in enumerate(clusters, 1):
            print(f"🔍 分析第 {i}/{len(clusters)} 个分组...")
            topic = self.analyze_topic_for_cluster(cluster)
            topics.append(topic)

        return topics

    def format_topics_report(self, topics: List[Dict]) -> str:
        """格式化主题报告"""
        report = ["\n" + "="*80]
        report.append("📋 聊天主题聚类分析报告")
        report.append("="*80)

        for i, topic in enumerate(topics, 1):
            analysis = topic.get('topic_analysis', {})
            report.append(f"\n🔖 主题 #{i}")
            report.append(f"   ⏰ 时间区间: {topic['start']} ~ {topic['end']}")
            report.append(f"   💬 消息数量: {topic['message_count']} 条")
            report.append(f"   📌 主题: {analysis.get('topic', '未知')}")
            report.append(f"   🔑 关键词: {', '.join(analysis.get('keywords', []))}")
            report.append(f"   📝 总结: {analysis.get('summary', '')}")
            report.append(f"   👥 参与者: {', '.join(analysis.get('participants', []))}")
            report.append(f"   😊 情感: {analysis.get('sentiment', 'neutral')}")

        report.append("\n" + "="*80)
        return "\n".join(report)


def test_image_understanding(client, image_path: str):
    """测试图片理解功能"""
    print("\n" + "="*80)
    print("🖼️  测试 1: 图片理解")
    print("="*80)

    # 定义多个测试任务
    test_prompts = [
        "请详细描述这张图片的内容",
        "请识别图片中的所有文字（OCR）",
        "请分析这张图片的风格和色彩",
    ]

    for i, prompt in enumerate(test_prompts, 1):
        print(f"\n🔍 任务 {i}: {prompt}")
        print("-" * 80)

        try:
            result = client.image_understand(
                image=image_path,
                prompt=prompt
            )

            print(f"结果:\n{result['content']}")
            print(f"\n📊 Token 使用: {result['usage']}")

        except Exception as e:
            print(f"❌ 错误: {e}")

        print()


def test_topic_clustering(client, messages_json_path: str):
    """测试主题聚类分析"""
    print("\n" + "="*80)
    print("📊 测试 2: 长文本主题聚类分析")
    print("="*80)

    analyzer = MessageAnalyzer(client)

    # 加载消息
    print(f"📂 加载消息文件: {messages_json_path}")
    messages = analyzer.load_messages(messages_json_path)
    print(f"✅ 成功加载 {len(messages)} 条消息\n")

    # 询问时间窗口
    print("💡 提示: 时间窗口越小，分组越细粒度（建议 2-6 小时）")

    try:
        window_input = input("请输入时间窗口（小时）[默认: 3]: ").strip()
        window_hours = int(window_input) if window_input else 3
    except ValueError:
        window_hours = 3

    print(f"\n⏱️  使用时间窗口: {window_hours} 小时\n")

    # 执行分析
    topics = analyzer.analyze_topics(messages, window_hours=window_hours)

    # 生成报告
    report = analyzer.format_topics_report(topics)
    print(report)

    # 保存结果
    output_path = "topic_analysis_result.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(topics, f, ensure_ascii=False, indent=2)

    print(f"\n💾 分析结果已保存到: {output_path}")

    return topics


def main():
    """主测试函数"""
    print("🤖 智谱 AI 功能测试")
    print("="*80)

    # 检查 API Key
    api_key = os.getenv("ZHIPUAI_API_KEY")
    if not api_key:
        print("❌ 错误: 请在 .env 文件中设置 ZHIPUAI_API_KEY")
        print("获取 API Key: https://open.bigmodel.cn/")
        return

    # 创建客户端
    client = create_zhipu_client(api_key)

    # 测试 1: 图片理解
    image_path = "test_case/5A7656747ADD083CB4F19E7FACEDB717.jpg"
    if os.path.exists(image_path):
        test_image_understanding(client, image_path)
    else:
        print(f"⚠️  测试图片不存在: {image_path}")

    # 测试 2: 主题聚类
    messages_json_path = "reports/group_insight/20260410-004112-有氧运动聊天/snapshot/messages.json"
    if os.path.exists(messages_json_path):
        test_topic_clustering(client, messages_json_path)
    else:
        print(f"⚠️  消息文件不存在: {messages_json_path}")

    print("\n✅ 测试完成!")


if __name__ == "__main__":
    main()
