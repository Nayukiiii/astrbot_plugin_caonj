import asyncio
import json
import math
import os
import random
import secrets
import time
from datetime import datetime

# ============================================================
# 草nj - 指令名称（在这里修改）
# ============================================================
CMD_CAONJ         = "草nj"       # 触发草nj
CMD_CAONJ_RANKING = "草nj排行"   # 查看谁草的最多
CMD_CAONJ_GRAPH   = "草nj关系图" # 今日草nj关系图
CMD_NJ_BATTLE     = "nj战绩"     # nj注入战绩排行
CMD_OUTSIDE_RANK  = "杂鱼排行"   # 选外面的杂鱼排行
CMD_RESET_CAONJ   = "重置草nj"   # 重置今日草nj次数
# ============================================================

import json as _json
import random as _random_mod

def _secrets_roll() -> float:
    """返回 [0, 1) 的密码学真随机浮点数"""
    return secrets.randbelow(100000) / 100000.0


def _calc_fancao_prob(
    fake_pct: int,
    times_today: int,
    user_30d_count: int,
    fancao_base: float,
) -> float:
    """
    计算反草触发概率。
    - Q：逃脱质量（sigmoid），越低越濒死
    - G：累积仇恨，被Q部分压制但轻松逃时保留残余
    - T：今日压力，同上
    - E：真随机熵扰动 ±0.06，替代时间因子
    - cap：上限随base线性开放（base=50→0.92，base=100→0.98）
    """
    Q = 1.0 / (1.0 + math.exp(-(fake_pct - 50) / 12.0))
    G = (1.0 - math.exp(-user_30d_count / 15.0)) * ((1.0 - Q) ** 2 + 0.04)
    T = (1.0 - math.exp(-times_today / 4.0)) * ((1.0 - Q) ** 1.5 + 0.02)
    E = (secrets.randbelow(10000) / 10000.0 - 0.5) * 0.12
    base_rate = fancao_base / 100.0
    cap = 0.88 + (fancao_base / 100.0) * 0.10
    raw = base_rate * ((1.0 - Q) ** 2 + G * 0.5 + T * 0.3) + E
    return max(0.01, min(cap, raw))


def _roll_injection_ml(fake_pct: int | None, grudge: float) -> float:
    """
    对数正态双峰注入量。
    - fake_pct=None 表示草nj（非反草），用中性参数
    - 反草时 fake_pct 越低（越濒死）均值越高；grudge 越高方差越大
    - 上限 100L，sigma 收紧使极大值极难触发
    """
    if fake_pct is None:
        # 草nj注入：中性对数正态，中位数约 200 mL
        mu    = 5.3
        sigma = 1.0
    else:
        Q     = 1.0 / (1.0 + math.exp(-(fake_pct - 50) / 12.0))
        # 情绪失控概率：濒死 × 仇恨
        berserk_prob = (1.0 - Q) * grudge
        mode_roll    = secrets.randbelow(10000) / 10000.0
        if mode_roll < berserk_prob:
            # 模式A：情绪失控，高均值高方差
            mu    = 5.3 + (1.0 - Q) * 1.5
            sigma = 1.0 + grudge * 0.5
        else:
            # 模式B：精准报复，中等均值低方差
            mu    = 4.8 + grudge * 0.5
            sigma = 0.6 + Q * 0.3
    raw = random.lognormvariate(mu=mu, sigma=sigma)
    return round(max(0.5, min(raw, 100000.0)), 1)


def _ml_grade(ml: float) -> str:
    """根据注入量返回评级字符串（字符由用户自定义，这里只做分档）"""
    if ml >= 4000:
        return "雕王"
    elif ml >= 2000:
        return "半雕王"
    elif ml >= 800:
        return "半死不死建议死"
    elif ml >= 400:
        return "杂鱼杂鱼就只有这点吗真是杂～鱼～欧～尼～酱～"
    elif ml >= 200:
        return "杂鱼杂鱼就只有这点吗真是杂～鱼～欧～尼～酱～"
    else:
        return "杂鱼杂鱼就只有这点吗真是杂～鱼～欧～尼～酱～"


def _load_comments(json_path: str) -> list:
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            return _json.load(f).get("ml_tiers", [])
    except Exception:
        return []

def _pick_comment(tiers: list, ml: float) -> str | None:
    for tier in tiers:
        if ml >= tier["min_ml"]:
            comments = [c for c in tier.get("comments", []) if c]
            if comments:
                return _random_mod.choice(comments)
            return None
    return None

def _load_fancao_comments(json_path: str) -> list:
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            return _json.load(f).get("grudge_tiers", [])
    except Exception:
        return []

def _pick_fancao_comment(tiers: list, grudge: float) -> str | None:
    for tier in tiers:
        if grudge >= tier["min_grudge"]:
            comments = [c for c in tier.get("comments", []) if c]
            if comments:
                return _random_mod.choice(comments)
            return None
    return None

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
from .nj_body_render import render_nj_body as _render_nj_body
from .nj_battle_render import render_nj_battle as _render_nj_battle
from .outside_rank_render import render_outside_rank as _render_outside_rank


def _fmt_ml(ml: float) -> str:
    """将 ml 数值格式化为公制可读字符串"""
    if ml < 1.0:
        return f"{ml * 1000:.0f} µL"
    elif ml < 1000.0:
        return f"{ml:.1f} mL"
    else:
        return f"{ml / 1000:.2f} L"


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

        # 反草等待状态 {group_id: {user_id: True}}
        self._fancao_pending: dict[str, dict[str, bool]] = {}
        # 反草meta {group_id: {user_id: {fake_pct, grudge}}}，独立存放
        self._fancao_meta: dict[str, dict[str, dict]] = {}

        # 重置尝试次数 {date: {group_id: {user_id: int}}}，内存维护，重启归零
        self._reset_attempts: dict[str, dict[str, dict[str, int]]] = {}

        # nj战绩数据文件（30天，记录nj注入了谁多少量）
        self.nj_battle_file = os.path.join(self.data_dir, "nj_battle.json")
        self.nj_battle_data = load_json(self.nj_battle_file, {})

        # 杂鱼数据文件（30天，记录选外面的次数和放弃量）
        self.outside_stats_file = os.path.join(self.data_dir, "outside_stats.json")
        self.outside_stats_data = load_json(self.outside_stats_file, {})

        self._body_comments    = _load_comments(os.path.join(self.curr_dir, "nj_body_comments.json"))
        self._battle_comments  = _load_comments(os.path.join(self.curr_dir, "nj_battle_comments.json"))
        self._fancao_comments  = _load_fancao_comments(os.path.join(self.curr_dir, "nj_fancao_comments.json"))

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

        # 概率判定（真随机）
        caonj_prob = float(self.config.get("caonj_probability", 30))
        caonj_prob = max(0.0, min(100.0, caonj_prob))
        if _secrets_roll() >= caonj_prob / 100.0:
            fake_pct = secrets.randbelow(99) + 1   # 1~99，仅用于显示和反草计算

            # 反草判定
            fancao_base = float(self.config.get("fancao_probability", 50))
            fancao_base = max(0.0, min(100.0, fancao_base))

            if fancao_base > 0:
                # 取今日被草记录数（今日群内总压力）
                times_today    = len(self._get_caonj_group_records(group_id))
                # 取该用户30天内草nj成功次数（仇恨值）
                user_30d_count = len(self.caonj_stats.get(group_id, {}).get(user_id, []))

                p_fancao = _calc_fancao_prob(fake_pct, times_today, user_30d_count, fancao_base)

                if _secrets_roll() < p_fancao:
                    # 反草成功
                    user_name = event.get_sender_name() or f"用户({user_id})"
                    nj_name   = self.config.get("nj_name", "宁隽")
                    grudge = min(1.0, user_30d_count / 15.0)
                    if group_id not in self._fancao_pending:
                        self._fancao_pending[group_id] = {}
                    self._fancao_pending[group_id][user_id] = True
                    # 保存fake_pct和grudge供注入量计算用
                    if group_id not in self._fancao_meta:
                        self._fancao_meta[group_id] = {}
                    self._fancao_meta[group_id][user_id] = {
                        "fake_pct": fake_pct,
                        "grudge": grudge,
                    }

                    _fancao_comment = _pick_fancao_comment(self._fancao_comments, grudge)
                    text = (
                        f" {_fancao_comment or 'nj不甘示弱，反草成功！🔥'}\n"
                        f"【{nj_name}】反草了【{user_name}】！\n\n"
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
                    return

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
            ml    = _roll_injection_ml(fake_pct=None, grudge=0.0)
            grade = _ml_grade(ml)
            self._record_nj_body(group_id, user_id, ml)
            _body_comment = _pick_comment(self._body_comments, ml)
            text = (
                f" 【{user_name}】选择了射在里面！\n"
                f"本次注入量：{_fmt_ml(ml)}　评级：{grade}\n"
                + (_body_comment if _body_comment else f"{nj_name} 感觉热热的~")
            )
        else:
            # 偷偷roll一次量做统计，但不注入
            _outside_ml = _roll_injection_ml(fake_pct=None, grudge=0.0)
            self._record_outside(group_id, user_id, _outside_ml)
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
    # 反草内外选择监听
    # ============================================================

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def fancao_choice_listener(self, event: AstrMessageEvent):
        """监听反草内外选择的回复（nj草发起者）"""
        if event.is_private_chat():
            return

        group_id = str(event.get_group_id())
        user_id  = str(event.get_sender_id())

        if not self._fancao_pending.get(group_id, {}).get(user_id):
            return

        msg = event.message_str.strip()
        if msg not in ("里面", "外面"):
            return

        # 清除等待状态，取出meta（fake_pct / grudge）
        del self._fancao_pending[group_id][user_id]
        meta   = self._fancao_meta.get(group_id, {}).pop(user_id, {})
        f_pct  = meta.get("fake_pct", 50)
        grudge = meta.get("grudge", 0.0)

        user_name = event.get_sender_name() or f"用户({user_id})"
        nj_name   = self.config.get("nj_name", "宁隽")

        if msg == "里面":
            ml    = _roll_injection_ml(fake_pct=f_pct, grudge=grudge)
            grade = _ml_grade(ml)
            # 记录nj战绩（nj注入了该user）
            self._record_nj_battle(group_id, user_id, ml)
            _battle_comment = _pick_comment(self._battle_comments, ml)
            text = (
                f" 【{user_name}】选择了让nj射在里面！\n"
                f"【{nj_name}】本次注入量：{_fmt_ml(ml)}　评级：{grade}\n"
                + (_battle_comment if _battle_comment else f"【{user_name}】感觉热热的~")
            )
        else:
            _outside_ml = _roll_injection_ml(fake_pct=f_pct, grudge=grudge)
            self._record_outside(group_id, user_id, _outside_ml)
            text = (
                f" 【{user_name}】选择了让nj射在外面！✨\n"
                f"【{nj_name}】松了一口气~"
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
    # nj战绩数据辅助
    # ============================================================

    def _record_nj_battle(self, group_id: str, victim_id: str, ml: float) -> None:
        """记录nj反草注入量（30天滚动，按 victim 统计）"""
        now = time.time()
        if group_id not in self.nj_battle_data:
            self.nj_battle_data[group_id] = {}
        gdata = self.nj_battle_data[group_id]
        if victim_id not in gdata:
            gdata[victim_id] = {"records": []}
        gdata[victim_id]["records"].append({"ts": now, "ml": ml})
        self._clean_nj_battle()
        save_json(self.nj_battle_file, self.nj_battle_data)

    def _clean_nj_battle(self) -> None:
        """清理30天前的nj战绩记录"""
        now = time.time()
        cutoff = 30 * 24 * 3600
        new_data = {}
        for gid, users in self.nj_battle_data.items():
            new_users = {}
            for uid, udata in users.items():
                valid = [r for r in udata.get("records", []) if now - r["ts"] < cutoff]
                if valid:
                    new_users[uid] = {"records": valid}
            if new_users:
                new_data[gid] = new_users
        self.nj_battle_data = new_data

    # ============================================================
    # 杂鱼数据辅助
    # ============================================================

    def _record_outside(self, group_id: str, user_id: str, ml: float) -> None:
        """记录选外面的次数和放弃量（30天滚动）"""
        now = time.time()
        if group_id not in self.outside_stats_data:
            self.outside_stats_data[group_id] = {}
        gdata = self.outside_stats_data[group_id]
        if user_id not in gdata:
            gdata[user_id] = {"records": []}
        gdata[user_id]["records"].append({"ts": now, "ml": ml})
        self._clean_outside()
        save_json(self.outside_stats_file, self.outside_stats_data)

    def _clean_outside(self) -> None:
        """清理30天前的杂鱼记录"""
        now = time.time()
        cutoff = 30 * 24 * 3600
        new_data = {}
        for gid, users in self.outside_stats_data.items():
            new_users = {}
            for uid, udata in users.items():
                valid = [r for r in udata.get("records", []) if now - r["ts"] < cutoff]
                if valid:
                    new_users[uid] = {"records": valid}
            if new_users:
                new_data[gid] = new_users
        self.outside_stats_data = new_data

    # ============================================================
    # /nj战绩
    # ============================================================

    @filter.command(CMD_NJ_BATTLE)
    async def nj_battle(self, event: AstrMessageEvent):
        async for result in self._cmd_nj_battle(event):
            yield result

    async def _cmd_nj_battle(self, event: AstrMessageEvent):
        if event.is_private_chat():
            yield event.plain_result("此功能仅在群聊中可用哦~")
            return

        group_id = str(event.get_group_id())
        if not is_allowed_group(group_id, self.config):
            return

        self._clean_nj_battle()
        gdata = self.nj_battle_data.get(group_id, {})

        if not gdata:
            yield event.plain_result("近30天nj还没有成功反草过任何人，nj很老实~")
            return

        # 拉群昵称
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

        # 聚合数据
        ranking = []
        for uid, udata in gdata.items():
            records = udata.get("records", [])
            total_ml = sum(r["ml"] for r in records)
            count    = len(records)
            ranking.append({
                "uid":   uid,
                "name":  user_map.get(uid, f"用户({uid})"),
                "count": count,
                "_ml_raw": total_ml,
                "ml":    _fmt_ml(total_ml),
            })

        ranking_by_ml    = sorted(ranking, key=lambda x: x["_ml_raw"], reverse=True)[:10]
        ranking_by_count = sorted(ranking, key=lambda x: x["count"],   reverse=True)[:10]

        nj_qq   = self.config.get("nj_qq",   "")
        nj_name = self.config.get("nj_name", "宁隽")

        import tempfile
        tmp_path = tempfile.mktemp(suffix=".png")
        try:
            await _render_nj_battle(
                nj_qq=nj_qq,
                nj_name=nj_name,
                ranking_by_ml=ranking_by_ml,
                ranking_by_count=ranking_by_count,
                out_path=tmp_path,
                cache_dir=os.path.join(self.curr_dir, "avatar_cache"),
                titles_path=os.path.join(self.curr_dir, "nj_battle_titles.json"),
            )
            yield event.image_result(tmp_path)
        except Exception as e:
            logger.error(f"渲染nj战绩失败: {e}")
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    # ============================================================
    # /杂鱼排行
    # ============================================================

    @filter.command(CMD_OUTSIDE_RANK)
    async def outside_rank(self, event: AstrMessageEvent):
        async for result in self._cmd_outside_rank(event):
            yield result

    async def _cmd_outside_rank(self, event: AstrMessageEvent):
        if event.is_private_chat():
            yield event.plain_result("此功能仅在群聊中可用哦~")
            return

        group_id = str(event.get_group_id())
        if not is_allowed_group(group_id, self.config):
            return

        self._clean_outside()
        gdata = self.outside_stats_data.get(group_id, {})

        if not gdata:
            yield event.plain_result("近30天还没有人在外面射过，大家都很勇敢~")
            return

        # 拉群昵称
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

        # 聚合数据
        ranking = []
        for uid, udata in gdata.items():
            records  = udata.get("records", [])
            total_ml = sum(r["ml"] for r in records)
            count    = len(records)
            ranking.append({
                "uid":     uid,
                "name":    user_map.get(uid, f"用户({uid})"),
                "count":   count,
                "_ml_raw": total_ml,
                "ml":      _fmt_ml(total_ml),
            })

        ranking_by_count = sorted(ranking, key=lambda x: x["count"],   reverse=True)[:10]
        ranking_by_ml    = sorted(ranking, key=lambda x: x["_ml_raw"], reverse=True)[:10]

        nj_qq   = self.config.get("nj_qq",   "")
        nj_name = self.config.get("nj_name", "宁隽")

        import tempfile
        tmp_path = tempfile.mktemp(suffix=".png")
        try:
            await _render_outside_rank(
                nj_qq=nj_qq,
                nj_name=nj_name,
                ranking_by_count=ranking_by_count,
                ranking_by_ml=ranking_by_ml,
                out_path=tmp_path,
                cache_dir=os.path.join(self.curr_dir, "avatar_cache"),
                titles_path=os.path.join(self.curr_dir, "outside_titles.json"),
            )
            yield event.image_result(tmp_path)
        except Exception as e:
            logger.error(f"渲染杂鱼排行失败: {e}")
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    # ============================================================
    # /重置草nj
    # ============================================================

    @filter.command(CMD_RESET_CAONJ)
    async def reset_caonj(self, event: AstrMessageEvent):
        async for result in self._cmd_reset_caonj(event):
            yield result

    async def _cmd_reset_caonj(self, event: AstrMessageEvent):
        if event.is_private_chat():
            yield event.plain_result("此功能仅在群聊中可用哦~")
            return

        group_id = str(event.get_group_id())
        if not is_allowed_group(group_id, self.config):
            return

        user_id   = str(event.get_sender_id())
        today     = datetime.now().strftime("%Y-%m-%d")
        nj_name   = self.config.get("nj_name", "宁隽")

        # 判断是否 bot 管理员
        is_admin = False
        try:
            admins = self.context.config_helper.admins or []
            is_admin = str(user_id) in [str(a) for a in admins]
        except Exception:
            pass

        # 解析 at 目标（管理员专用）
        at_target: str | None = None
        reset_all = False
        raw = event.message_str.strip()
        if raw == "全员":
            reset_all = True
        else:
            # 尝试从消息链取第一个 At
            for seg in event.get_messages():
                if hasattr(seg, "qq") and str(seg.qq) != user_id:
                    at_target = str(seg.qq)
                    break

        # 确保今日caonj_daily存在
        if self.caonj_daily.get("date") != today:
            self.caonj_daily = {"date": today, "groups": {}}

        # ── 管理员逻辑 ──────────────────────────────────────
        if is_admin:
            if reset_all:
                self.caonj_daily["groups"][group_id] = {}
                save_json(self.caonj_daily_file, self.caonj_daily)
                yield event.plain_result(f"已重置本群所有人今日草{nj_name}次数~")
                return

            target_id = at_target or user_id
            if group_id in self.caonj_daily["groups"]:
                self.caonj_daily["groups"][group_id].pop(target_id, None)
            save_json(self.caonj_daily_file, self.caonj_daily)
            if target_id == user_id:
                yield event.plain_result(f"已重置你今日草{nj_name}的次数~")
            else:
                yield event.plain_result(f"已重置 {target_id} 今日草{nj_name}的次数~")
            return

        # ── 普通用户逻辑（概率+次数限制）──────────────────
        if at_target or reset_all:
            yield event.plain_result("你没有权限重置他人的次数哦~")
            return

        max_attempts = int(self.config.get("reset_daily_attempts", 3))

        # 取今日已用尝试次数
        day_attempts = self._reset_attempts.setdefault(today, {}).setdefault(group_id, {})
        used = day_attempts.get(user_id, 0)

        if used >= max_attempts:
            yield event.plain_result(f"你今天已经尝试了 {max_attempts} 次，机会用完了~")
            return

        # 扣除一次尝试
        day_attempts[user_id] = used + 1
        remaining = max_attempts - used - 1

        # 掷骰
        caonj_prob = float(self.config.get("caonj_probability", 30))
        caonj_prob = max(0.0, min(100.0, caonj_prob))

        if _secrets_roll() < caonj_prob / 100.0:
            # 成功
            if group_id in self.caonj_daily["groups"]:
                self.caonj_daily["groups"][group_id].pop(user_id, None)
            save_json(self.caonj_daily_file, self.caonj_daily)
            yield event.plain_result(
                f"重置成功！今日草{nj_name}次数已清零~\n"
                f"（今日剩余尝试次数：{remaining}）"
            )
        else:
            yield event.plain_result(
                f"重置失败，{nj_name}不配合你~\n"
                f"（今日剩余尝试次数：{remaining}）"
            )

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

        _raw_ranking = sorted(
            [
                {
                    "uid": uid,
                    "name": user_map.get(uid, f"用户({uid})"),
                    "count": d["count"],
                    "_ml_raw": d["ml"],
                }
                for uid, d in users_data.items()
            ],
            key=lambda x: x["_ml_raw"],
            reverse=True,
        )
        ranking = [
            {**r, "ml": _fmt_ml(r["_ml_raw"])} for r in _raw_ranking
        ][:10]

        import tempfile
        tmp_path = tempfile.mktemp(suffix=".png")
        try:
            await _render_nj_body(
                nj_qq=nj_qq,
                nj_name=nj_name,
                total_ml_str=_fmt_ml(total_ml),
                total_count=total_count,
                reset_date=reset_date,
                days_left=days_left,
                hours_left=hours_left,
                ranking=ranking,
                out_path=tmp_path,
                cache_dir=os.path.join(self.curr_dir, "avatar_cache"),
                titles_path=os.path.join(self.curr_dir, "nj_body_titles.json"),
            )
            yield event.image_result(tmp_path)
        except Exception as e:
            logger.error(f"渲染nj体内失败: {e}")
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    # ============================================================
    # 插件卸载清理
    # ============================================================

    async def terminate(self):
        save_json(self.caonj_stats_file,   self.caonj_stats)
        save_json(self.caonj_records_file, self.caonj_records)
        save_json(self.caonj_daily_file,   self.caonj_daily)
        save_json(self.nj_body_file,       self.nj_body_data)
        save_json(self.nj_battle_file,     self.nj_battle_data)

        for task in tuple(self._withdraw_tasks):
            task.cancel()
        self._withdraw_tasks.clear()