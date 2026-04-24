"""
测试 pywechat 在一次对话中给多个目标发送信息
功能：给「测试群1」和「测试群2」发送测试消息（群聊多目标转发）
"""
import sys
import time

# 将 pywechat 路径添加到 Python 路径
sys.path.insert(0, r'c:\Users\XQH\Downloads\wechat-ability\pywechat')

from pyweixin.WeChatAuto import Messages
from pyweixin.WeChatTools import Navigator, Tools
from pyweixin import GlobalConfig


def setup_global_config():
    """配置全局参数以优化搜索体验"""
    # 增加加载延迟，给搜索更多时间
    GlobalConfig.load_delay = 2.0  # 默认1.5秒，增加到2秒
    # 减少发送延迟
    GlobalConfig.send_delay = 0.3
    # 不关闭微信
    GlobalConfig.close_weixin = False
    # 不最大化窗口
    GlobalConfig.is_maximize = False


def send_to_filehelper(message):
    """专门处理文件传输助手的发送"""
    try:
        print(f"\n📨 正在发送给：文件传输助手")

        # 文件传输助手可能需要从会话列表中查找，而不是搜索
        # 先尝试在会话列表中查找
        try:
            # type: ignore (search_pages应该是int，但pywechat类型注解错误写成bool)
            Messages.send_messages_to_friend(
                friend="文件传输助手",
                messages=[message],
                search_pages=5,  # type: ignore
                send_delay=0.3,
                is_maximize=False,
                close_weixin=False
            )
            print(f"   ✅ 成功发送给：文件传输助手")
            return True
        except Exception as e:
            print(f"   ⚠️  会话列表查找失败，尝试顶部搜索：{str(e)}")

            # 如果会话列表查找失败，尝试顶部搜索
            time.sleep(1.0)  # 额外等待1秒

            # type: ignore (search_pages应该是int，但pywechat类型注解错误写成bool)
            Messages.send_messages_to_friend(
                friend="文件传输助手",
                messages=[message],
                search_pages=0,  # type: ignore
                send_delay=0.3,
                is_maximize=False,
                close_weixin=False
            )
            print(f"   ✅ 成功发送给：文件传输助手（通过搜索）")
            return True

    except Exception as e:
        print(f"   ❌ 发送给文件传输助手失败：{str(e)}")
        print(f"   💡 建议：请手动在微信中打开一次'文件传输助手'会话")
        return False


def send_to_group_chat(friend, message):
    """发送给群聊：优先顶部全局搜索，失败则回退到会话列表翻页"""
    # 尝试方式1：顶部全局搜索（对群聊最可靠）
    try:
        print(f"\n📨 方式1（顶部搜索）正在发送给：{friend}")
        Messages.send_messages_to_friend(  # type: ignore
            friend=friend,
            messages=[message],
            search_pages=0,  # type: ignore
            send_delay=0.3,
            is_maximize=False,
            close_weixin=False
        )
        print(f"   ✅ 成功发送给：{friend}")
        return True
    except Exception as e:
        print(f"   ⚠️  方式1失败：{str(e)}")

    # 尝试方式2：会话列表翻页查找
    try:
        print(f"\n📨 方式2（会话列表翻页）正在发送给：{friend}")
        Messages.send_messages_to_friend(  # type: ignore
            friend=friend,
            messages=[message],
            search_pages=5,  # type: ignore
            send_delay=0.3,
            is_maximize=False,
            close_weixin=False
        )
        print(f"   ✅ 成功发送给：{friend}")
        return True
    except Exception as e:
        print(f"   ❌ 方式2也失败：{str(e)}")
        return False


def try_send_with_fallback(friend, message):
    """
    智能发送函数：尝试多种方式发送消息
    适用于文件传输助手等特殊功能
    """
    # 尝试方式1：直接在会话列表查找（适用于最近联系过的目标）
    try:
        print(f"\n📨 尝试方式1：会话列表查找 - {friend}")
        # type: ignore
        Messages.send_messages_to_friend(
            friend=friend,
            messages=[message],
            search_pages=5,  # type: ignore
            send_delay=0.3,
            is_maximize=False,
            close_weixin=False
        )
        print(f"   ✅ 方式1成功：{friend}")
        return True
    except Exception as e:
        print(f"   ⚠️  方式1失败：{str(e)}")

    # 尝试方式2：使用Navigator直接打开会话（适用于特殊功能）
    try:
        print(f"\n📨 尝试方式2：Navigator直接打开 - {friend}")
        from pyweixin.Uielements import Edits as EditsClass
        from pyweixin.WinSettings import SystemSettings

        # 实例化Edits类
        Edits = EditsClass()

        main_window = Navigator.open_dialog_window(
            friend=friend,
            is_maximize=False,
            search_pages=0
        )

        # 手动输入和发送
        edit_area = main_window.child_window(**Edits.CurrentChatEdit)
        if edit_area.exists(timeout=1.0):
            edit_area.click_input()
            SystemSettings.copy_text_to_clipboard(message)
            import pyautogui
            pyautogui.hotkey('ctrl', 'v')
            time.sleep(0.3)
            pyautogui.hotkey('alt', 's')
            print(f"   ✅ 方式2成功：{friend}")
            return True
        else:
            print(f"   ⚠️  方式2失败：找不到输入框")
    except Exception as e:
        print(f"   ⚠️  方式2失败：{str(e)}")

    # 尝试方式3：顶部搜索（适用于普通好友）
    try:
        print(f"\n📨 尝试方式3：顶部搜索 - {friend}")
        time.sleep(0.5)  # 等待之前的操作完成
        # type: ignore
        Messages.send_messages_to_friend(
            friend=friend,
            messages=[message],
            search_pages=0,  # type: ignore
            send_delay=0.3,
            is_maximize=False,
            close_weixin=False
        )
        print(f"   ✅ 方式3成功：{friend}")
        return True
    except Exception as e:
        print(f"   ❌ 方式3失败：{str(e)}")
        return False


def test_send_to_multiple_targets():
    """测试给多个目标发送消息"""

    # 检查微信是否运行
    if not Tools.is_weixin_running():
        print("❌ 错误：微信未运行，请先启动并登录微信")
        return False

    print("✅ 微信正在运行")

    # 定义发送目标和对应的消息
    targets = {
        "测试群1": "【自动化测试】这是一条发往测试群1的消息，用于验证多群聊转发功能。",
        "测试群2": "【自动化测试】这是一条发往测试群2的消息，用于验证多群聊转发功能。"
    }

    print(f"\n📤 准备向 {len(targets)} 个目标发送消息：")
    print("=" * 60)
    for i, (target, message) in enumerate(targets.items(), 1):
        print(f"\n目标 {i}：{target}")
        print(f"消息内容：{message}")
        print("-" * 60)

    # 等待用户确认
    print("\n" + "=" * 60)
    print("⚠️  即将开始发送消息！")
    print("=" * 60)
    print("📌 请确认：")
    print("  1. 微信正在运行并已登录")
    print("  2. 目标联系人名称正确")
    print("  3. 消息内容无误")
    print("=" * 60)
    confirm = input("\n⏸️  输入 'yes' 开始发送，其他任意键取消: ").strip().lower()

    if confirm != 'yes':
        print("\n❌ 用户取消发送")
        return False

    print("\n🔄 开始发送消息...")

    # 配置全局参数
    setup_global_config()

    # 使用专门的函数处理每个目标
    success_count = 0
    for target, message in targets.items():
        if send_to_group_chat(target, message):
            success_count += 1
        time.sleep(1.5)  # 每个目标之间间隔1.5秒，避免操作过快

    print(f"\n🎉 发送完成！成功：{success_count}/{len(targets)}")
    return success_count == len(targets)


def test_send_batch_method():
    """测试使用批量发送方法"""

    # 检查微信是否运行
    if not Tools.is_weixin_running():
        print("❌ 错误：微信未运行，请先启动并登录微信")
        return False

    print("✅ 微信正在运行")

    # 定义发送目标和消息（注意：批量方法要求每个目标的消息列表对应）
    friends = ["测试群1", "测试群2"]
    messages = [
        ["【批量测试】这是发往测试群1的批量消息"],
        ["【批量测试】这是发往测试群2的批量消息"]
    ]

    print(f"\n📤 使用批量发送方法向 {len(friends)} 个目标发送消息")
    print("=" * 60)
    for i, (friend, msg_list) in enumerate(zip(friends, messages), 1):
        print(f"\n目标 {i}：{friend}")
        print(f"消息内容：{msg_list[0]}")
        print("-" * 60)

    # 等待用户确认
    print("\n" + "=" * 60)
    print("⚠️  即将开始批量发送消息！")
    print("=" * 60)
    print("📌 请确认：")
    print("  1. 微信正在运行并已登录")
    print("  2. 所有目标联系人名称正确")
    print("  3. 消息内容无误")
    print("=" * 60)
    confirm = input("\n⏸️  输入 'yes' 开始发送，其他任意键取消: ").strip().lower()

    if confirm != 'yes':
        print("\n❌ 用户取消发送")
        return False

    print("\n🔄 开始批量发送...")

    try:
        # 方式2：批量发送（一次性处理多个目标）
        # type: ignore (send_messages_to_friends的参数类型注解可能有误)
        Messages.send_messages_to_friends(
            friends=friends,
            messages=messages,
            send_delay=0.3,
            is_maximize=False,
            close_weixin=False
        )

        print("\n🎉 批量发送完成！")
        return True

    except Exception as e:
        print(f"\n❌ 批量发送失败：{str(e)}")
        return False


def test_open_conversations():
    """测试打开目标会话窗口"""

    # 检查微信是否运行
    if not Tools.is_weixin_running():
        print("❌ 错误：微信未运行，请先启动并登录微信")
        return False

    print("✅ 微信正在运行")

    targets = ["测试群1", "测试群2"]

    print(f"\n🔍 测试打开 {len(targets)} 个目标会话：")
    print("=" * 60)
    for i, target in enumerate(targets, 1):
        print(f"\n目标 {i}：{target}")

    # 等待用户确认
    print("\n" + "=" * 60)
    print("⚠️  即将开始打开会话窗口！")
    print("=" * 60)
    print("📌 请确认：")
    print("  1. 微信正在运行并已登录")
    print("  2. 所有目标联系人名称正确")
    print("=" * 60)
    confirm = input("\n⏸️  输入 'yes' 开始打开，其他任意键取消: ").strip().lower()

    if confirm != 'yes':
        print("\n❌ 用户取消操作")
        return False

    print("\n🔄 开始打开会话窗口...")

    for target in targets:
        try:
            print(f"\n📱 打开会话：{target}")

            # 使用 Navigator 打开会话窗口
            main_window = Navigator.open_dialog_window(
                friend=target,
                is_maximize=False,
                search_pages=0  # 直接搜索
            )

            print(f"   ✅ 成功打开：{target}")

            # 激活输入框
            from pyweixin.Uielements import Edits
            edit_area = main_window.child_window(**Edits.CurrentChatEdit)
            if edit_area.exists(timeout=0.5):
                edit_area.click_input()
                print(f"   ✅ 输入框已激活")

            time.sleep(1)  # 等待1秒，方便观察

        except Exception as e:
            print(f"   ❌ 打开 {target} 失败：{str(e)}")
            continue

    print("\n🎉 会话窗口测试完成！")
    return True


if __name__ == "__main__":
    print("=" * 60)
    print("🧪 pywechat 多目标发送测试")
    print("=" * 60)

    # 选择测试模式
    print("\n请选择测试模式：")
    print("1. 逐个发送消息（推荐，需要确认）")
    print("2. 批量发送消息（需要确认）")
    print("3. 仅打开会话窗口（不发送消息，需要确认）")
    print("\n⚠️  注意：所有模式都会在执行前要求确认！")

    choice = input("\n请输入选择 (1/2/3, 默认1): ").strip() or "1"

    if choice == "1":
        print("\n🔄 模式：逐个发送消息")
        test_send_to_multiple_targets()

    elif choice == "2":
        print("\n🔄 模式：批量发送消息")
        test_send_batch_method()

    elif choice == "3":
        print("\n🔄 模式：仅打开会话窗口")
        test_open_conversations()

    else:
        print("\n❌ 无效的选择，使用默认模式（逐个发送）")
        test_send_to_multiple_targets()

    print("\n" + "=" * 60)
    print("测试结束")
    print("=" * 60)
