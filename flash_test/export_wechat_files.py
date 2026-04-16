"""
微信文件导出工具

功能：
1. 扫描聊天记录中的文件消息（app_type=6）
2. 通过时间戳和文件名匹配本地文件
3. 复制文件到指定目录

支持：图片、视频、文件、语音
"""
import os
import re
import shutil
import sqlite3
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

class WeChatFileExporter:
    """微信文件导出器"""

    def __init__(
        self,
        wechat_id: str,
        decrypted_dir: str,
        output_dir: str,
        wechat_files_path: Optional[str] = None
    ):
        """
        初始化

        Args:
            wechat_id: 微信ID（如 wxid_xxx）
            decrypted_dir: 解密后的数据库目录
            output_dir: 导出目标目录
            wechat_files_path: 微信文件存储路径（默认自动检测）
        """
        self.wechat_id = wechat_id
        self.decrypted_dir = Path(decrypted_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 自动检测微信文件路径
        if wechat_files_path:
            self.wechat_files_path = Path(wechat_files_path)
        else:
            self.wechat_files_path = self._detect_wechat_files_path()

        # 文件存储子目录
        self.storage_dirs = {
            "image": self.wechat_files_path / "FileStorage" / "Image",
            "video": self.wechat_files_path / "FileStorage" / "Video",
            "file": self.wechat_files_path / "FileStorage" / "File",
            "voice": self.wechat_files_path / "FileStorage" / "Voice",
        }

    def _detect_wechat_files_path(self) -> Path:
        """自动检测微信文件存储路径"""
        # Windows 默认路径
        doc_path = Path.home() / "Documents" / "WeChat Files"
        if doc_path.exists():
            return doc_path / self.wechat_id

        # 如果找不到，返回当前目录
        return Path.cwd()

    def scan_file_messages(
        self,
        chat_name: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
    ) -> List[Dict]:
        """
        扫描文件消息

        Args:
            chat_name: 联系人/群聊名称（None=所有）
            start_time: 开始时间（格式：YYYY-MM-DD HH:MM:SS）
            end_time: 结束时间

        Returns:
            文件消息列表
        """
        file_messages = []

        # 扫描所有 message_*.db
        for db_path in self.decrypted_dir.glob("message/message_*.db"):
            try:
                messages = self._scan_single_db(
                    db_path,
                    chat_name=chat_name,
                    start_time=start_time,
                    end_time=end_time
                )
                file_messages.extend(messages)
            except Exception as e:
                print(f"⚠️  扫描 {db_path.name} 失败: {e}")

        # 按时间排序
        file_messages.sort(key=lambda x: x.get("create_time", 0))

        return file_messages

    def _scan_single_db(
        self,
        db_path: Path,
        chat_name: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
    ) -> List[Dict]:
        """扫描单个数据库"""
        messages = []

        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)

            # 获取所有消息表
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'Msg_%'"
            ).fetchall()

            for (table_name,) in tables:
                try:
                    # 查询文件消息（app_type=6）
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
                        # 解析 appmsg
                        file_info = self._parse_file_message(content, create_time)

                        if file_info:
                            file_info["db_table"] = table_name
                            file_info["local_id"] = local_id
                            messages.append(file_info)

                except Exception as e:
                    print(f"⚠️  查询表 {table_name} 失败: {e}")
                    continue

            conn.close()

        except Exception as e:
            print(f"⚠️  打开数据库失败: {e}")

        return messages

    def _parse_file_message(self, content: str, create_time: int) -> Optional[Dict]:
        """解析文件消息内容"""
        if not content or "<appmsg" not in content:
            return None

        try:
            # 提取 app_type
            app_type_match = re.search(r'<type>(\d+)</type>', content)
            if not app_type_match:
                return None

            app_type = int(app_type_match.group(1))

            # 只处理文件类型（app_type=6）
            if app_type != 6:
                return None

            # 提取标题（文件名）
            title_match = re.search(r'<title><!\[CDATA\[(.*?)\]\]></title>', content)
            title = title_match.group(1) if title_match else "未知文件"

            # 提取摘要（文件大小）
            summary_match = re.search(r'<des><!\[CDATA\[(.*?)\]\]></des>', content)
            summary = summary_match.group(1) if summary_match else ""

            return {
                "title": title,
                "summary": summary,
                "create_time": create_time,
                "create_time_readable": self._timestamp_to_readable(create_time),
                "app_type": app_type,
            }

        except Exception as e:
            print(f"⚠️  解析消息失败: {e}")
            return None

    def _timestamp_to_readable(self, timestamp: int) -> str:
        """时间戳转可读格式"""
        try:
            # 微信时间戳是毫秒
            dt = datetime.fromtimestamp(timestamp / 1000)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return "未知时间"

    def find_and_export_files(
        self,
        file_messages: List[Dict],
        dry_run: bool = True
    ) -> List[Dict]:
        """
        查找并导出文件

        Args:
            file_messages: 文件消息列表
            dry_run: 预演模式（不实际复制）

        Returns:
            导出结果列表
        """
        results = []

        for msg in file_messages:
            result = {
                "message": msg,
                "found": False,
                "source_path": None,
                "target_path": None,
            }

            # 查找文件
            source_path = self._find_file(msg)

            if source_path and source_path.exists():
                result["found"] = True
                result["source_path"] = str(source_path)

                # 构建目标路径
                filename = source_path.name
                date_dir = self._get_date_dir(msg["create_time"])
                target_path = self.output_dir / date_dir / filename
                result["target_path"] = str(target_path)

                # 复制文件
                if not dry_run:
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source_path, target_path)
                    print(f"✅ 已复制: {filename}")
                else:
                    print(f"🔍 找到: {filename}")

            else:
                print(f"❌ 未找到: {msg['title']}")

            results.append(result)

        return results

    def _find_file(self, msg: Dict) -> Optional[Path]:
        """查找文件"""
        filename = msg["title"]
        create_time = msg["create_time"]

        # 尝试在不同目录中查找
        search_dirs = [
            self.storage_dirs["file"],
            self.storage_dirs["image"],
            self.storage_dirs["video"],
            self.storage_dirs["voice"],
        ]

        for storage_dir in search_dirs:
            if not storage_dir.exists():
                continue

            # 按日期目录查找
            date_dir = self._get_date_dir(create_time)
            target_dir = storage_dir / date_dir

            if target_dir.exists():
                # 查找文件
                matches = list(target_dir.rglob(filename))

                if matches:
                    # 返回最匹配的文件（按修改时间）
                    matches.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                    return matches[0]

            # 全局搜索
            matches = list(storage_dir.rglob(filename))
            if matches:
                matches.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                return matches[0]

        return None

    def _get_date_dir(self, create_time: int) -> str:
        """获取日期目录（格式：YYYY-MM）"""
        try:
            dt = datetime.fromtimestamp(create_time / 1000)
            return dt.strftime("%Y-%m")
        except:
            return "unknown"

    def export_report(self, results: List[Dict], output_path: Optional[str] = None):
        """生成导出报告"""
        if not output_path:
            output_path = self.output_dir / "export_report.json"

        report = {
            "total": len(results),
            "found": sum(1 for r in results if r["found"]),
            "not_found": sum(1 for r in results if not r["found"]),
            "files": [
                {
                    "title": r["message"]["title"],
                    "create_time": r["message"]["create_time_readable"],
                    "found": r["found"],
                    "source": r["source_path"],
                    "target": r["target_path"],
                }
                for r in results
            ],
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print(f"\n📊 报告已保存到: {output_path}")
        print(f"   总计: {report['total']}")
        print(f"   找到: {report['found']}")
        print(f"   未找到: {report['not_found']}")


def main():
    """主函数示例"""
    # 配置
    config = {
        "wechat_id": "wxid_ldxssnix94gz22",  # 替换为你的微信ID
        "decrypted_dir": r"C:\Users\XQH\Downloads\wechat-ability\wechat-decrypt\decrypted",
        "output_dir": r"C:\Users\XQH\Downloads\wechat-ability\exported_files",
        "chat_name": None,  # None=所有聊天，或指定联系人/群聊名称
    }

    # 创建导出器
    exporter = WeChatFileExporter(
        wechat_id=config["wechat_id"],
        decrypted_dir=config["decrypted_dir"],
        output_dir=config["output_dir"]
    )

    print("🔍 扫描文件消息...")
    file_messages = exporter.scan_file_messages(
        chat_name=config["chat_name"]
    )

    print(f"📦 找到 {len(file_messages)} 个文件消息")

    if file_messages:
        print("\n🔍 查找本地文件...")
        results = exporter.find_and_export_files(
            file_messages,
            dry_run=False  # 设为 True 预演，不实际复制
        )

        print("\n📊 生成报告...")
        exporter.export_report(results)

    print("\n✅ 完成！")


if __name__ == "__main__":
    main()
