import asyncio
import json
import os
import random
import time
from datetime import datetime

# ============================================================
# 草nj - 指令名称（在这里修改）
# ============================================================
CMD_CAONJ         = "草nj"       # 触发草nj
CMD_CAONJ_RANKING = "草nj排行"   # 查看谁草的最多
CMD_CAONJ_GRAPH   = "草nj关系图" # 今日草nj关系图
# ============================================================

import astrbot.api.message_components as Comp
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)
from astrbot.core.utils.astrbot_path import get_astrbot_plugin_data_path

from .onebot_api import extract_message_id
from .src.utils import (
    load_json,
    save_json,
    is_allowed_group,
    resolve_member_name,
)


class CaonjPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.config = config

        self.curr_dir = os.path.dirname(__file__)

        self._withdraw_tasks: set[asyncio.Task] = set()

        # 独立数据目录
        self.data_dir = os.path.join(get_astrbot_plugin_data_path(), "caonj_plugin")
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir, exist_ok=True)

        # 草nj数据文件
        self.caonj_stats_file   = os.path.join(self.data_dir, "caonj_stats.json")
        self.caonj_records_file = os.path.join(self.data_dir, "caonj_records.json")
        self.caonj_daily_file   = os.path.join(self.data_dir, "caonj_daily.json")
        self.nj_body_file       = os.path.join(self.data_dir, "nj_body.json")

        self.caonj_stats   = load_json(self.caonj_stats_file,   {})
        self.caonj_records = load_json(self.caonj_records_file, {"date": "", "groups": {}})
        self.caonj_daily   = load_json(self.caonj_daily_file,   {"date": "", "groups": {}})
        self.nj_body_data  = load_json(self.nj_body_file,       {})

        # 草nj内外选择等待状态 {group_id: {user_id: True}}
        self._caonj_pending: dict[str, dict[str, bool]] = {}

        logger.info(f"草nj插件已加载。数据目录: {self.data_dir}")

    # ============================================================
    # OneBot 辅助
    # ============================================================

    def _auto_withdraw_enabled(self) -> bool:
        return bool(self.config.get("auto_withdraw_enabled", False))

    def _auto_withdraw_delay_seconds(self) -> int:
        return int(self.config.get("auto_withdraw_delay_seconds", 5))

    def _can_onebot_withdraw(self, event: AstrMessageEvent) -> bool:
        if not self._auto_withdraw_enabled():
            return False
        return (
            event.get_platform_name() == "aiocqhttp"
            and isinstance(event, AiocqhttpMessageEvent)
        )

    async def _send_onebot_message(
        self, event: AstrMessageEvent, *, message: list[dict]
    ) -> object:
        assert isinstance(event, AiocqhttpMessageEvent)
        group_id = event.get_group_id()
        if group_id:
            resp = await event.bot.api.call_action(
                "send_group_msg", group_id=int(group_id), message=message
            )
        else:
            resp = await event.bot.api.call_action(
                "send_private_msg",
                user_id=int(event.get_sender_id()),
                message=message,
            )
        message_id = extract_message_id(resp)
        return message_id

    def _schedule_onebot_delete_msg(self, client, *, message_id: object) -> None:
        delay = self._auto_withdraw_delay_seconds()

        async def _runner():
            await asyncio.sleep(delay)
            try:
                await client.api.call_action("delete_msg", message_id=message_id)
            except Exception as e:
                logger.warning(f"自动撤回失败: {e}")

        task = asyncio.create_task(_runner())
        self._withdraw_tasks.add(task)
        task.add_done_callback(self._withdraw_tasks.discard)

    # ============================================================
    # 草nj 数据辅助
    # ============================================================

    def _ensure_today_caonj_records(self) -> None:
        today = datetime.now().strftime("%Y-%m-%d")
        if self.caonj_records.get("date") != today:
            self.caonj_records = {"date": today, "groups": {}}

    def _get_caonj_group_records(self, group_id: str) -> list:
        self._ensure_today_caonj_records()
        if group_id not in self.caonj_records["groups"]:
            self.caonj_records["groups"][group_id] = {"records": []}
        return self.caonj_records["groups"][group_id]["records"]

    def _clean_caonj_stats(self) -> None:
        """清理30天前的草nj记录"""
        now = time.time()
        thirty_days = 30 * 24 * 3600
        new_stats = {}
        for gid, users in self.caonj_stats.items():
            new_users = {}
            for uid, ts_list in users.items():
                valid = [ts for ts in ts_list if now - ts < thirty_days]
                if valid:
                    new_users[uid] = valid
            if new_users:
                new_stats[gid] = new_users
        self.caonj_stats = new_stats
        save_json(self.caonj_stats_file, self.caonj_stats)

    def _record_nj_body(self, group_id: str, user_id: str, ml: float) -> None:
        """记录nj体内注入量（月度统计）"""
        today = datetime.now()
        reset_date = today.strftime("%Y-%m-01")

        if group_id not in self.nj_body_data:
            self.nj_body_data[group_id] = {
                "total_ml": 0.0, "count": 0,
                "last_reset": reset_date, "users": {}
            }

        gdata = self.nj_body_data[group_id]

        # 月刷新检查
        if gdata.get("last_reset", "") != reset_date:
            gdata["total_ml"] = 0.0
            gdata["count"] = 0
            gdata["users"] = {}
            gdata["last_reset"] = reset_date

        gdata["total_ml"] = round(gdata.get("total_ml", 0.0) + ml, 1)
        gdata["count"] = gdata.get("count", 0) + 1

        if user_id not in gdata["users"]:
            gdata["users"][user_id] = {"count": 0, "ml": 0.0}
        gdata["users"][user_id]["count"] += 1
        gdata["users"][user_id]["ml"] = round(gdata["users"][user_id]["ml"] + ml, 1)

        save_json(self.nj_body_file, self.nj_body_data)

    # ============================================================
    # /草nj
    # ============================================================

    @filter.command(CMD_CAONJ)
    async def caonj(self, event: AstrMessageEvent):
        async for result in self._cmd_caonj(event):
            yield result

    async def _cmd_caonj(self, event: AstrMessageEvent):
        if event.is_private_chat():
            yield event.plain_result("此功能仅在群聊中可用哦~")
            return

        group_id = str(event.get_group_id())
        if not is_allowed_group(group_id, self.config):
            return

        user_id = str(event.get_sender_id())

        # 每日次数限制检查
        today = datetime.now().strftime("%Y-%m-%d")
        if self.caonj_daily.get("date") != today:
            self.caonj_daily = {"date": today, "groups": {}}
        daily_limit = int(self.config.get("caonj_daily_limit", 5))
        used_today = self.caonj_daily["groups"].get(group_id, {}).get(user_id, 0)
        if used_today >= daily_limit:
            yield event.plain_result(f"你今天已经草了 {daily_limit} 次nj了，nj受不了啦，明天再来吧~")
            return

        # 概率判定
        caonj_prob = float(self.config.get("caonj_probability", 30))
        caonj_prob = max(0.0, min(100.0, caonj_prob))
        if random.uniform(0, 100) > caonj_prob:
            fake_pct = random.randint(1, 99)
            yield event.plain_result(f"在 {fake_pct}% 的时候被nj逃走了")
            return

        # 触发成功 —— 获取发送者昵称
        user_name = event.get_sender_name() or f"用户({user_id})"
        members = []
        try:
            if event.get_platform_name() == "aiocqhttp":
                assert isinstance(event, AiocqhttpMessageEvent)
                members = await event.bot.api.call_action(
                    "get_group_member_list", group_id=int(group_id)
                )
                if (
                    isinstance(members, dict)
                    and "data" in members
                    and isinstance(members["data"], list)
                ):
                    members = members["data"]
                user_name = resolve_member_name(members, user_id=user_id, fallback=user_name)
        except Exception:
            pass

        # 标记今日已用（+1）
        if group_id not in self.caonj_daily["groups"]:
            self.caonj_daily["groups"][group_id] = {}
        self.caonj_daily["groups"][group_id][user_id] = used_today + 1
        save_json(self.caonj_daily_file, self.caonj_daily)

        # 统计记录（30天维度）
        if group_id not in self.caonj_stats:
            self.caonj_stats[group_id] = {}
        if user_id not in self.caonj_stats[group_id]:
            self.caonj_stats[group_id][user_id] = []
        self.caonj_stats[group_id][user_id].append(time.time())
        self._clean_caonj_stats()
        save_json(self.caonj_stats_file, self.caonj_stats)

        # 今日关系图记录
        group_caonj_records = self._get_caonj_group_records(group_id)
        group_caonj_records.append({
            "user_id": user_id,
            "user_name": user_name,
            "timestamp": datetime.now().isoformat(),
        })
        save_json(self.caonj_records_file, self.caonj_records)

        # 进入等待内外选择状态
        if group_id not in self._caonj_pending:
            self._caonj_pending[group_id] = {}
        self._caonj_pending[group_id][user_id] = True

        remaining = daily_limit - (used_today + 1)
        text = (
            f" 草nj成功！🎉\n【{user_name}】今天草了nj！\n"
            f"今日剩余次数：{remaining} 次\n\n"
            f"请选择：回复【里面】或【外面】"
        )

        if self._can_onebot_withdraw(event):
            message_id = await self._send_onebot_message(
                event,
                message=[
                    {"type": "at", "data": {"qq": user_id}},
                    {"type": "text", "data": {"text": text}},
                ],
            )
            if message_id is not None:
                self._schedule_onebot_delete_msg(event.bot, message_id=message_id)
            return

        yield event.chain_result([Comp.At(qq=user_id), Comp.Plain(text)])

    # ============================================================
    # 内外选择监听
    # ============================================================

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def caonj_choice_listener(self, event: AstrMessageEvent):
        """监听草nj内外选择的回复"""
        if event.is_private_chat():
            return

        group_id = str(event.get_group_id())
        user_id = str(event.get_sender_id())

        if not self._caonj_pending.get(group_id, {}).get(user_id):
            return

        msg = event.message_str.strip()
        if msg not in ("里面", "外面"):
            return

        # 清除等待状态
        del self._caonj_pending[group_id][user_id]

        user_name = event.get_sender_name() or f"用户({user_id})"
        nj_name = self.config.get("nj_name", "宁隽")

        if msg == "里面":
            ml = round(random.uniform(5.0, 5000.0), 1)
            self._record_nj_body(group_id, user_id, ml)
            text = (
                f" 【{user_name}】选择了射在里面！\n"
                f"本次注入量：{ml} ml\n"
                f"{nj_name} 感觉热热的~"
            )
        else:
            text = (
                f" 【{user_name}】选择了射在外面！✨\n"
                f"{nj_name} 松了一口气~"
            )

        if self._can_onebot_withdraw(event):
            await self._send_onebot_message(
                event,
                message=[
                    {"type": "at", "data": {"qq": user_id}},
                    {"type": "text", "data": {"text": text}},
                ],
            )
            event.stop_event()
            return

        yield event.chain_result([Comp.At(qq=user_id), Comp.Plain(text)])
        event.stop_event()

    # ============================================================
    # /草nj排行
    # ============================================================

    @filter.command(CMD_CAONJ_RANKING)
    async def caonj_ranking(self, event: AstrMessageEvent):
        async for result in self._cmd_caonj_ranking(event):
            yield result

    async def _cmd_caonj_ranking(self, event: AstrMessageEvent):
        if event.is_private_chat():
            yield event.plain_result("私聊看不了榜单哦~")
            return

        group_id = str(event.get_group_id())
        self._clean_caonj_stats()

        group_data = self.caonj_stats.get(group_id, {})
        if not group_data:
            yield event.plain_result("本群近30天还没有人草过nj，大家都很守规矩呢。")
            return

        user_map = {}
        try:
            if event.get_platform_name() == "aiocqhttp":
                members = await event.bot.api.call_action(
                    "get_group_member_list", group_id=int(group_id)
                )
                if isinstance(members, dict) and "data" in members:
                    members = members["data"]
                for m in members:
                    uid = str(m.get("user_id"))
                    user_map[uid] = m.get("card") or m.get("nickname") or uid
        except Exception:
            pass

        sorted_list = sorted(
            [{"uid": uid, "name": user_map.get(uid, f"用户({uid})"), "count": len(ts_list)}
             for uid, ts_list in group_data.items()],
            key=lambda x: x["count"], reverse=True
        )[:10]

        current_rank = 1
        for i, user in enumerate(sorted_list):
            if i > 0 and user["count"] < sorted_list[i - 1]["count"]:
                current_rank = i + 1
            user["rank"] = current_rank

        template_path = os.path.join(self.curr_dir, "caonj_ranking.html")
        if not os.path.exists(template_path):
            yield event.plain_result("错误：找不到排行模板 caonj_ranking.html")
            return

        with open(template_path, "r", encoding="utf-8") as f:
            template_content = f.read()

        header_h, item_h, footer_h, rank_width = 100, 62, 50, 400
        nj_profile_h = 110
        dynamic_height = header_h + nj_profile_h + (len(sorted_list) * item_h) + footer_h

        try:
            url = await self.html_render(
                template_content,
                {
                    "group_id": group_id,
                    "ranking": sorted_list,
                    "title": "🔥 草nj月榜 🔥",
                    "nj_qq": self.config.get("nj_qq", ""),
                    "nj_name": self.config.get("nj_name", "宁隽"),
                },
                options={
                    "type": "png", "quality": None, "full_page": False,
                    "clip": {"x": 0, "y": 0, "width": rank_width, "height": dynamic_height},
                    "scale": "device", "device_scale_factor_level": "ultra",
                },
            )
            yield event.image_result(url)
        except Exception as e:
            logger.error(f"渲染草nj排行失败: {e}")

    # ============================================================
    # /草nj关系图
    # ============================================================

    @filter.command(CMD_CAONJ_GRAPH)
    async def caonj_graph(self, event: AstrMessageEvent):
        async for result in self._cmd_caonj_graph(event):
            yield result

    async def _cmd_caonj_graph(self, event: AstrMessageEvent):
        if event.is_private_chat():
            yield event.plain_result("此功能仅在群聊中可用哦~")
            return

        group_id = str(event.get_group_id())
        if not is_allowed_group(group_id, self.config):
            return

        self._ensure_today_caonj_records()
        group_caonj_records = self.caonj_records.get("groups", {}).get(group_id, {}).get("records", [])

        if not group_caonj_records:
            yield event.plain_result("今天还没有人草过nj哦~")
            return

        group_name = "未命名群聊"
        user_map = {}
        try:
            if event.get_platform_name() == "aiocqhttp":
                assert isinstance(event, AiocqhttpMessageEvent)
                info = await event.bot.api.call_action("get_group_info", group_id=int(group_id))
                if isinstance(info, dict) and "data" in info:
                    info = info["data"]
                group_name = info.get("group_name", "未命名群聊")

                members = await event.bot.api.call_action(
                    "get_group_member_list", group_id=int(group_id)
                )
                if isinstance(members, dict) and "data" in members:
                    members = members["data"]
                if isinstance(members, list):
                    for m in members:
                        uid = str(m.get("user_id"))
                        user_map[uid] = m.get("card") or m.get("nickname") or uid
        except Exception as e:
            logger.warning(f"获取群信息失败: {e}")

        vis_js_path = os.path.join(self.curr_dir, "vis-network.min.js")
        vis_js_content = ""
        if os.path.exists(vis_js_path):
            with open(vis_js_path, "r", encoding="utf-8") as f:
                vis_js_content = f.read()

        template_path = os.path.join(self.curr_dir, "caonj_graph_template.html")
        if not os.path.exists(template_path):
            yield event.plain_result("错误：找不到模板文件 caonj_graph_template.html")
            return

        with open(template_path, "r", encoding="utf-8") as f:
            graph_html = f.read()

        unique_nodes = {r["user_id"] for r in group_caonj_records}
        node_count = len(unique_nodes) + 1  # +1 for nj 中心节点
        clip_width = 1920
        clip_height = 1080 + max(0, node_count - 10) * 60
        iter_count = self.config.get("iterations", 140)

        try:
            url = await self.html_render(
                graph_html,
                {
                    "vis_js_content": vis_js_content,
                    "group_id": group_id,
                    "group_name": group_name,
                    "user_map": user_map,
                    "records": group_caonj_records,
                    "iterations": iter_count,
                },
                options={
                    "type": "png", "quality": None, "scale": "device",
                    "clip": {"x": 0, "y": 0, "width": clip_width, "height": clip_height},
                    "full_page": False, "device_scale_factor_level": "ultra",
                },
            )
            yield event.image_result(url)
        except Exception as e:
            logger.error(f"渲染草nj关系图失败: {e}")

    # ============================================================
    # /nj体内
    # ============================================================

    @filter.command("nj体内")
    async def nj_body(self, event: AstrMessageEvent):
        async for result in self._cmd_nj_body(event):
            yield result

    async def _cmd_nj_body(self, event: AstrMessageEvent):
        """查询nj体内液体"""
        if event.is_private_chat():
            yield event.plain_result("此功能仅在群聊中可用哦~")
            return

        group_id = str(event.get_group_id())
        if not is_allowed_group(group_id, self.config):
            return

        nj_qq   = self.config.get("nj_qq", "")
        nj_name = self.config.get("nj_name", "宁隽")

        today = datetime.now()
        reset_date = today.strftime("%Y-%m-01")

        gdata = self.nj_body_data.get(group_id, {})

        # 月刷新检查
        if gdata.get("last_reset", "") != reset_date:
            gdata = {"total_ml": 0.0, "count": 0, "last_reset": reset_date, "users": {}}
            self.nj_body_data[group_id] = gdata
            save_json(self.nj_body_file, self.nj_body_data)

        total_ml    = gdata.get("total_ml", 0.0)
        total_count = gdata.get("count", 0)
        users_data  = gdata.get("users", {})

        # 计算下次刷新时间
        if today.month == 12:
            next_reset = datetime(today.year + 1, 1, 1)
        else:
            next_reset = datetime(today.year, today.month + 1, 1)
        delta      = next_reset - today
        days_left  = delta.days
        hours_left = delta.seconds // 3600

        # 构建排行
        user_map = {}
        try:
            if event.get_platform_name() == "aiocqhttp":
                assert isinstance(event, AiocqhttpMessageEvent)
                members = await event.bot.api.call_action(
                    "get_group_member_list", group_id=int(group_id)
                )
                if isinstance(members, dict) and "data" in members and isinstance(members["data"], list):
                    members = members["data"]
                for m in members:
                    uid = str(m.get("user_id"))
                    user_map[uid] = m.get("card") or m.get("nickname") or uid
        except Exception:
            pass

        ranking = sorted(
            [
                {
                    "uid": uid,
                    "name": user_map.get(uid, f"用户({uid})"),
                    "count": d["count"],
                    "ml": d["ml"],
                }
                for uid, d in users_data.items()
            ],
            key=lambda x: x["ml"],
            reverse=True,
        )

        template_path = os.path.join(self.curr_dir, "nj_body.html")
        if not os.path.exists(template_path):
            yield event.plain_result("错误：找不到模板文件 nj_body.html")
            return

        with open(template_path, "r", encoding="utf-8") as f:
            template_content = f.read()

        header_h        = 80
        profile_h       = 130
        stats_h         = 90
        reset_h         = 50
        ranking_header_h = 40 if ranking else 0
        item_h          = 66
        footer_h        = 36
        empty_h         = 50 if not ranking else 0
        dynamic_height  = (
            header_h + profile_h + stats_h + reset_h
            + ranking_header_h + (len(ranking) * item_h)
            + empty_h + footer_h
        )

        try:
            url = await self.html_render(
                template_content,
                {
                    "nj_qq": nj_qq,
                    "nj_name": nj_name,
                    "total_ml": total_ml,
                    "total_count": total_count,
                    "reset_date": reset_date,
                    "days_left": days_left,
                    "hours_left": hours_left,
                    "ranking": ranking,
                },
                options={
                    "type": "png", "quality": None, "full_page": False,
                    "clip": {"x": 0, "y": 0, "width": 420, "height": dynamic_height},
                    "scale": "device", "device_scale_factor_level": "ultra",
                },
            )
            yield event.image_result(url)
        except Exception as e:
            logger.error(f"渲染nj体内失败: {e}")

    # ============================================================
    # 插件卸载清理
    # ============================================================

    async def terminate(self):
        save_json(self.caonj_stats_file,   self.caonj_stats)
        save_json(self.caonj_records_file, self.caonj_records)
        save_json(self.caonj_daily_file,   self.caonj_daily)
        save_json(self.nj_body_file,       self.nj_body_data)

        for task in tuple(self._withdraw_tasks):
            task.cancel()
        self._withdraw_tasks.clear()
