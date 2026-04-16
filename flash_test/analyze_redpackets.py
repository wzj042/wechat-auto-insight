"""
微信红包信息分析工具

功能：
1. 扫描聊天记录中的红包消息（app_type=2001）
2. 统计谁给谁发了红包
3. 生成红包报告

注意：
- 无法获取红包金额
- 无法获取领取状态
- 只能确定发送者和接收者（专属红包）
"""
import os
import re
import sqlite3
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from collections import defaultdict, Counter


class WeChatRedPacketAnalyzer:
    """微信红包分析器"""

    def __init__(self, decrypted_dir: str):
        """
        初始化

        Args:
            decrypted_dir: 解密后的数据库目录
        """
        self.decrypted_dir = Path(decrypted_dir)

    def analyze(
        self,
        chat_name: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
    ) -> Dict:
        """
        分析红包信息

        Args:
            chat_name: 联系人/群聊名称（None=所有）
            start_time: 开始时间（格式：YYYY-MM-DD HH:MM:SS）
            end_time: 结束时间

        Returns:
            分析结果
        """
        redpackets = self._scan_redpackets(
            chat_name=chat_name,
            start_time=start_time,
            end_time=end_time
        )

        if not redpackets:
            return {
                "total": 0,
                "by_sender": {},
                "by_receiver": {},
                "direct_redpackets": [],
                "group_redpackets": [],
            }

        # 统计发送者
        by_sender = Counter()
        for rp in redpackets:
            sender = rp.get("redpacket_sender_name", "未知")
            by_sender[sender] += 1

        # 统计接收者（专属红包）
        by_receiver = Counter()
        direct_redpackets = []
        group_redpackets = []

        for rp in redpackets:
            if rp.get("interaction_kind") == "direct_redpacket":
                receiver = rp.get("redpacket_receiver_name", "未知")
                by_receiver[receiver] += 1
                direct_redpackets.append(rp)
            else:
                group_redpackets.append(rp)

        return {
            "total": len(redpackets),
            "by_sender": dict(by_sender.most_common()),
            "by_receiver": dict(by_receiver.most_common()),
            "direct_redpackets": direct_redpackets,
            "group_redpackets": group_redpackets,
            "all_redpackets": redpackets,
        }

    def _scan_redpackets(
        self,
        chat_name: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
    ) -> List[Dict]:
        """扫描红包消息"""
        redpackets = []

        # 扫描所有 message_*.db
        for db_path in self.decrypted_dir.glob("message/message_*.db"):
            try:
                messages = self._scan_single_db(
                    db_path,
                    chat_name=chat_name,
                    start_time=start_time,
                    end_time=end_time
                )
                redpackets.extend(messages)
            except Exception as e:
                print(f"⚠️  扫描 {db_path.name} 失败: {e}")

        # 按时间排序
        redpackets.sort(key=lambda x: x.get("create_time", 0))

        return redpackets

    def _scan_single_db(
        self,
        db_path: Path,
        chat_name: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
    ) -> List[Dict]:
        """扫描单个数据库"""
        redpackets = []

        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)

            # 加载联系人名称映射
            names = self._load_contact_names(conn)

            # 获取所有消息表
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'Msg_%'"
            ).fetchall()

            for (table_name,) in tables:
                try:
                    # 查询红包消息（local_type=49）
                    query = f"""
                        SELECT
                            local_id,
                            local_type,
                            create_time,
                            message_content
                        FROM [{table_name}]
                        WHERE local_type = 49
                        ORDER BY create_time DESC
                    """

                    rows = conn.execute(query).fetchall()

                    for local_id, local_type, create_time, content in rows:
                        # 解析红包消息
                        rp_info = self._parse_redpacket_message(
                            content,
                            create_time,
                            names
                        )

                        if rp_info:
                            redpackets.append(rp_info)

                except Exception as e:
                    print(f"⚠️  查询表 {table_name} 失败: {e}")
                    continue

            conn.close()

        except Exception as e:
            print(f"⚠️  打开数据库失败: {e}")

        return redpackets

    def _load_contact_names(self, conn: sqlite3.Connection) -> Dict[str, str]:
        """加载联系人名称映射"""
        names = {}

        try:
            # 从 contact 数据库加载
            contact_db = self.decrypted_dir / "contact" / "contact.db"
            if contact_db.exists():
                contact_conn = sqlite3.connect(f"file:{contact_db}?mode=ro", uri=True)
                rows = contact_conn.execute(
                    "SELECT userName, remark, nickname FROM Contact"
                ).fetchall()

                for username, remark, nickname in rows:
                    display_name = remark or nickname or username
                    names[username] = display_name

                contact_conn.close()

        except Exception as e:
            print(f"⚠️  加载联系人失败: {e}")

        return names

    def _parse_redpacket_message(
        self,
        content: str,
        create_time: int,
        names: Dict[str, str]
    ) -> Optional[Dict]:
        """解析红包消息内容"""
        if not content or "<appmsg" not in content:
            return None

        try:
            # 提取 app_type
            app_type_match = re.search(r'<type>(\d+)</type>', content)
            if not app_type_match:
                return None

            app_type = int(app_type_match.group(1))

            # 只处理红包类型（app_type=2001）
            if app_type != 2001:
                return None

            # 提取发送者
            sender_match = re.search(r'<fromusername><!\[CDATA\[(.*?)\]\]></fromusername>', content)
            sender_username = sender_match.group(1) if sender_match else ""

            # 从 native_url 提取发送者（备选）
            if not sender_username:
                url_match = re.search(r'<nativeurl><!\[CDATA\[(.*?)\]\]></nativeurl>', content)
                if url_match:
                    url = url_match.group(1)
                    # 从 URL 参数中提取 sendusername
                    sendusername_match = re.search(r'sendusername=([^&]+)', url)
                    if sendusername_match:
                        sender_username = sendusername_match.group(1)

            # 提取接收者（专属红包）
            receiver_match = re.search(
                r'<exclusive_recv_username><!\[CDATA\[(.*?)\]\]></exclusive_recv_username>',
                content
            )
            receiver_username = receiver_match.group(1) if receiver_match else ""

            # 提取标题
            title_match = re.search(r'<title><!\[CDATA\[(.*?)\]\]></title>', content)
            title = title_match.group(1) if title_match else "恭喜发财，大吉大利"

            # 提取留言
            memo_match = re.search(
                r'<receivertitle><!\[CDATA\[(.*?)\]\]></receivertitle>',
                content
            )
            memo = memo_match.group(1) if memo_match else ""

            return {
                "interaction_kind": "direct_redpacket" if receiver_username else "redpacket",
                "title": title,
                "redpacket_memo": memo,
                "redpacket_sender_username": sender_username,
                "redpacket_sender_name": names.get(sender_username, sender_username or "未知"),
                "redpacket_receiver_username": receiver_username,
                "redpacket_receiver_name": names.get(receiver_username, receiver_username or "群红包"),
                "create_time": create_time,
                "create_time_readable": self._timestamp_to_readable(create_time),
                "app_type": app_type,
            }

        except Exception as e:
            print(f"⚠️  解析红包消息失败: {e}")
            return None

    def _timestamp_to_readable(self, timestamp: int) -> str:
        """时间戳转可读格式"""
        try:
            # 微信时间戳是毫秒
            dt = datetime.fromtimestamp(timestamp / 1000)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return "未知时间"

    def generate_report(self, analysis: Dict, output_path: str):
        """生成红包报告"""
        report = {
            "summary": {
                "total_redpackets": analysis["total"],
                "direct_redpackets": len(analysis["direct_redpackets"]),
                "group_redpackets": len(analysis["group_redpackets"]),
            },
            "top_senders": analysis["by_sender"],
            "top_receivers": analysis["by_receiver"],
            "redpackets": [
                {
                    "time": rp["create_time_readable"],
                    "sender": rp["redpacket_sender_name"],
                    "receiver": rp.get("redpacket_receiver_name", "群红包"),
                    "title": rp["title"],
                    "memo": rp["redpacket_memo"],
                    "type": "专属红包" if rp.get("interaction_kind") == "direct_redpacket" else "群红包",
                }
                for rp in analysis["all_redpackets"]
            ]
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print(f"\n📊 红包报告已保存到: {output_path}")
        print(f"\n📈 统计摘要:")
        print(f"   总计: {report['summary']['total_redpackets']} 个红包")
        print(f"   专属红包: {report['summary']['direct_redpackets']} 个")
        print(f"   群红包: {report['summary']['group_redpackets']} 个")

        if analysis["by_sender"]:
            print(f"\n🎁 发送最多:")
            for sender, count in list(analysis["by_sender"].items())[:5]:
                print(f"   {sender}: {count} 个")

        if analysis["by_receiver"]:
            print(f"\n🎯 接收最多（专属红包）:")
            for receiver, count in list(analysis["by_receiver"].items())[:5]:
                print(f"   {receiver}: {count} 个")


def main():
    """主函数示例"""
    # 配置
    config = {
        "decrypted_dir": r"C:\Users\XQH\Downloads\wechat-ability\wechat-decrypt\decrypted",
        "chat_name": None,  # None=所有聊天，或指定群聊名称
        "output_file": r"C:\Users\XQH\Downloads\wechat-ability\redpacket_report.json",
    }

    # 创建分析器
    analyzer = WeChatRedPacketAnalyzer(
        decrypted_dir=config["decrypted_dir"]
    )

    print("🔍 分析红包信息...")
    analysis = analyzer.analyze(
        chat_name=config["chat_name"]
    )

    print(f"📦 找到 {analysis['total']} 个红包")

    if analysis["total"] > 0:
        print("\n📊 生成报告...")
        analyzer.generate_report(analysis, config["output_file"])

    print("\n✅ 完成！")


if __name__ == "__main__":
    main()
