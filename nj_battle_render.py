"""
nj战绩卡片 Pillow 渲染
风格：与 nj_body_render.py 保持一致（白底 / 粉色 accent / 方形像素阴影）
显示两列排行：按注入总量 + 按被注入次数
async 版本，内部自动下载 QQ 头像
"""
from PIL import Image, ImageDraw, ImageFont
import sys, os, io, asyncio
import aiohttp

_FONT_REG  = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"
_FONT_MED  = "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc"
_FONT_BOLD = "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc"

if sys.platform == "win32":
    _WIN = os.path.join(os.environ.get("WINDIR", "C:\\Windows"), "Fonts")
    for _var, _cands in [
        ("_FONT_REG",  ["msyh.ttc", "simsun.ttc", "Arial.ttf"]),
        ("_FONT_MED",  ["msyhbd.ttc", "msyh.ttc", "Arial.ttf"]),
        ("_FONT_BOLD", ["msyhbd.ttc", "simhei.ttf", "Arial.ttf"]),
    ]:
        for _c in _cands:
            _p = os.path.join(_WIN, _c)
            if os.path.exists(_p):
                globals()[_var] = _p
                break

def _font(path, size):
    try:    return ImageFont.truetype(path, size)
    except: return ImageFont.load_default()


def _load_titles(json_path: str) -> dict:
    """加载称号配置json，失败返回空结构"""
    try:
        import json
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"ml_tiers": [], "count_tiers": []}


def _pick_title(tiers: list, key_min: str, value) -> str | None:
    """从分段配置中匹配并随机抽一条称号，无匹配返回None"""
    import random
    for tier in tiers:
        if value >= tier[key_min]:
            titles = [t for t in tier.get("titles", []) if t]
            if titles:
                return random.choice(titles)
            return None
    return None


# 色彩系统（同 nj_body_render）
BG_PAGE      = (255, 245, 247)
BG_CONT      = (255, 255, 255)
BG_ALT       = (255, 250, 252)
SEP          = (255, 240, 245)
ACCENT       = (200,  90, 124)
ACCENT_LIGHT = (255, 183, 197)
ACCENT_SOFT  = (255, 214, 224)
DEEP         = (160,  54,  90)
TEXT_PRIMARY = ( 74,  63,  75)
TEXT_LIGHT   = (255, 255, 255)
TEXT_GRAY    = (138, 127, 139)
RANK1        = (255, 215,   0)
RANK2        = (192, 192, 192)
RANK3        = (205, 127,  50)

# 次数榜用略不同色调以示区分
ACCENT2       = (150,  80, 160)
ACCENT2_LIGHT = (210, 170, 230)
ACCENT2_SOFT  = (230, 200, 245)
DEEP2         = (100,  40, 130)


def _grad_h(draw, x0, y0, x1, y1, cl, cr):
    w = x1 - x0
    if w <= 0: return
    for i in range(w):
        t = i / (w - 1) if w > 1 else 0
        c = tuple(int(cl[k] + (cr[k] - cl[k]) * t) for k in range(3))
        draw.line([(x0+i, y0), (x0+i, y1)], fill=c)

def _grad_v(draw, x0, y0, x1, y1, ct, cb):
    h = y1 - y0
    if h <= 0: return
    for i in range(h):
        t = i / (h - 1) if h > 1 else 0
        c = tuple(int(ct[k] + (cb[k] - ct[k]) * t) for k in range(3))
        draw.line([(x0, y0+i), (x1, y0+i)], fill=c)

def _shadow(draw, x0, y0, x1, y1, s, col=None):
    draw.rectangle([x0+s, y0+s, x1+s, y1+s], fill=col or ACCENT_SOFT)

def _tw(draw, text, fnt):
    bb = draw.textbbox((0, 0), text, font=fnt); return bb[2] - bb[0]

def _th(draw, text, fnt):
    bb = draw.textbbox((0, 0), text, font=fnt); return bb[3] - bb[1]

def _tc(draw, cx, y, text, fnt, fill):
    draw.text((cx - _tw(draw, text, fnt) // 2, y), text, font=fnt, fill=fill)

def _tag(draw, x, y, text, fnt, cl, cr, s):
    PX, PY = 8*s, 4*s
    bw = _tw(draw, text, fnt) + PX*2
    bh = _th(draw, text, fnt) + PY*2
    _shadow(draw, x, y, x+bw, y+bh, 2*s)
    _grad_h(draw, x, y, x+bw, y+bh, cl, cr)
    draw.text((x+PX, y+PY), text, font=fnt, fill=TEXT_LIGHT)
    return bw, bh

def _paste_avatar(img, avatar_bytes, x, y, size):
    try:
        av = Image.open(io.BytesIO(avatar_bytes)).convert("RGB")
        av = av.resize((size, size), Image.LANCZOS)
        img.paste(av, (x, y))
    except Exception:
        pass

async def _fetch_qq_avatar(qq: str, cache_dir: str, size: int = 100) -> bytes | None:
    cache_path = os.path.join(cache_dir, f"avatar_{qq}.jpg")
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "rb") as f:
                return f.read()
        except Exception:
            pass
    url = f"https://q4.qlogo.cn/headimg_dl?dst_uin={qq}&spec={size}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    try:
                        os.makedirs(cache_dir, exist_ok=True)
                        with open(cache_path, "wb") as f:
                            f.write(data)
                    except Exception:
                        pass
                    return data
    except Exception:
        pass
    return None


def _draw_ranking_section(
    img, draw, results, S,
    start_y, section_w, offset_x,
    title, ranking, is_ml_rank,
    body_h,
    titles_cfg=None,
):
    """渲染单侧排行榜（注入量 or 次数）"""
    HDR_H  = 44 * S
    ITEM_H = 68 * S

    ac       = ACCENT       if is_ml_rank else ACCENT2
    ac_light = ACCENT_LIGHT if is_ml_rank else ACCENT2_LIGHT
    ac_soft  = ACCENT_SOFT  if is_ml_rank else ACCENT2_SOFT
    dp       = DEEP         if is_ml_rank else DEEP2

    y = start_y

    # 先把整个区域填白，避免两侧条目数不同时露出底色
    draw.rectangle([offset_x, y, offset_x + section_w, y + body_h], fill=BG_CONT)

    # 小节标题（纯文字，不用 emoji，NotoSansCJK 不支持 emoji）
    draw.rectangle([offset_x, y, offset_x + section_w, y + HDR_H], fill=BG_CONT)
    draw.line([(offset_x, y + HDR_H - 2*S), (offset_x + section_w, y + HDR_H - 2*S)],
              fill=ac_soft, width=2*S)
    fn = _font(_FONT_BOLD, 13*S)
    cx = offset_x + section_w // 2
    _tc(draw, cx, y + (HDR_H - _th(draw, "A", fn)) // 2, title, fn, ac)
    y += HDR_H

    if not ranking:
        EMPT_H = 60 * S
        draw.rectangle([offset_x, y, offset_x + section_w, y + EMPT_H], fill=BG_CONT)
        fe = _font(_FONT_REG, 11*S)
        _tc(draw, cx, y + (EMPT_H - _th(draw, "A", fe)) // 2,
            "暂无数据", fe, TEXT_GRAY)
        return HDR_H + EMPT_H

    RANK_SZ = [20*S, 18*S, 18*S]
    RANK_CL = [RANK1, RANK2, RANK3]
    ft      = _font(_FONT_REG, 10*S)

    for i, user in enumerate(ranking):
        iy = y + i * ITEM_H
        draw.rectangle([offset_x, iy, offset_x + section_w, iy + ITEM_H],
                       fill=BG_ALT if i % 2 == 0 else BG_CONT)
        draw.line([(offset_x, iy + ITEM_H - 2*S), (offset_x + section_w, iy + ITEM_H - 2*S)],
                  fill=SEP, width=2*S)

        # 排名数字
        fn_rk = _font(_FONT_BOLD, RANK_SZ[i] if i < 3 else 15*S)
        rc    = RANK_CL[i] if i < 3 else ac
        rnum  = str(i + 1)
        draw.text(
            (offset_x + 10*S + (22*S - _tw(draw, rnum, fn_rk)) // 2,
             iy + (ITEM_H - _th(draw, rnum, fn_rk)) // 2),
            rnum, font=fn_rk, fill=rc,
        )

        # 头像
        AV2 = 38*S
        avx = offset_x + 38*S
        avy = iy + (ITEM_H - AV2) // 2
        _shadow(draw, avx, avy, avx + AV2, avy + AV2, 3*S, ac_soft)
        av_data = results.get(user["uid"])
        if av_data:
            _paste_avatar(img, av_data, avx, avy, AV2)
            draw.rectangle([avx, avy, avx + AV2, avy + AV2], outline=ac_light, width=2*S)
        else:
            _grad_h(draw, avx, avy, avx + AV2, avy + AV2, ac_light, ac)
            draw.rectangle([avx, avy, avx + AV2, avy + AV2], outline=ac_light, width=2*S)
            fn_i = _font(_FONT_BOLD, 13*S)
            ini  = user["name"][0] if user["name"] else "?"
            _tc(draw, avx + AV2 // 2, avy + (AV2 - _th(draw, ini, fn_i)) // 2, ini, fn_i, TEXT_LIGHT)

        # 名字 + 称号标签
        fn_nm = _font(_FONT_MED, 11*S)
        ft_t  = _font(_FONT_REG,  9*S)
        nx    = avx + AV2 + 8*S
        name  = user["name"]
        # 截断过长名字
        while _tw(draw, name, fn_nm) > section_w - nx + offset_x - 8*S and len(name) > 1:
            name = name[:-1]
        if name != user["name"]:
            name = name[:-1] + "…"

        # 从json配置取称号
        _tc_cfg     = titles_cfg or {"ml_tiers": [], "count_tiers": []}
        ml_title    = _pick_title(_tc_cfg["ml_tiers"],    "min_ml",    user.get("_ml_raw", 0.0))
        count_title = _pick_title(_tc_cfg["count_tiers"], "min_count", user.get("count", 0))
        has_title   = ml_title or count_title

        if has_title:
            name_y = iy + ITEM_H // 2 - _th(draw, name, fn_nm) - 4*S
        else:
            name_y = iy + (ITEM_H - _th(draw, name, fn_nm)) // 2
        draw.text((nx, name_y), name, font=fn_nm, fill=TEXT_PRIMARY)

        if has_title:
            tx = nx
            ty = name_y + _th(draw, name, fn_nm) + 3*S
            TAG_PX, TAG_PY = 5*S, 2*S
            TAG_GAP = 4*S
            if ml_title:
                bw = _tw(draw, ml_title, ft_t) + TAG_PX*2
                bh = _th(draw, ml_title, ft_t) + TAG_PY*2
                _shadow(draw, tx, ty, tx+bw, ty+bh, 2*S, ACCENT_SOFT)
                _grad_h(draw, tx, ty, tx+bw, ty+bh, ACCENT_LIGHT, ACCENT)
                draw.text((tx+TAG_PX, ty+TAG_PY), ml_title, font=ft_t, fill=TEXT_LIGHT)
                tx += bw + TAG_GAP
            if count_title:
                bw2 = _tw(draw, count_title, ft_t) + TAG_PX*2
                bh2 = _th(draw, count_title, ft_t) + TAG_PY*2
                _shadow(draw, tx, ty, tx+bw2, ty+bh2, 2*S, ACCENT2_SOFT)
                _grad_h(draw, tx, ty, tx+bw2, ty+bh2, ACCENT2_LIGHT, ACCENT2)
                draw.text((tx+TAG_PX, ty+TAG_PY), count_title, font=ft_t, fill=TEXT_LIGHT)

        # 数值标签（右侧对齐，垂直居中）
        if is_ml_rank:
            val_txt = user["ml"]
            val_cl  = (ac, dp)
        else:
            val_txt = f"{user['count']} 次"
            val_cl  = (ac, dp)

        ft_v    = _font(_FONT_REG, 10*S)
        val_bw  = _tw(draw, val_txt, ft_v) + 14*S
        val_bh  = _th(draw, val_txt, ft_v) + 8*S
        val_x   = offset_x + section_w - val_bw - 10*S
        val_y   = iy + (ITEM_H - val_bh) // 2
        _shadow(draw, val_x, val_y, val_x+val_bw, val_y+val_bh, 2*S)
        _grad_h(draw, val_x, val_y, val_x+val_bw, val_y+val_bh, val_cl[0], val_cl[1])
        draw.text((val_x + 7*S, val_y + 4*S), val_txt, font=ft_v, fill=TEXT_LIGHT)

    return HDR_H + ITEM_H * len(ranking)


async def render_nj_battle(
    nj_qq, nj_name,
    ranking_by_ml,    # list of {uid, name, count, ml, _ml_raw}
    ranking_by_count, # same shape
    out_path,
    cache_dir=None,
    titles_path=None, # nj_battle_titles.json 路径，None 则不显示称号
    scale=2,
):
    S  = scale
    W  = 840 * S   # 双列布局，比 nj_body 宽一倍

    HALF_W  = W // 2
    GAP     = 4 * S   # 中间分割线宽

    HDR_H   = 54 * S
    PRO_H   = 130 * S
    FOOT_H  = 36 * S

    max_items  = max(len(ranking_by_ml), len(ranking_by_count), 1)
    ITEM_H     = 68 * S
    SEC_HDR_H  = 44 * S
    BODY_H     = SEC_HDR_H + ITEM_H * max_items

    H = HDR_H + PRO_H + BODY_H + FOOT_H

    # ── 并发下载所有头像 ──────────────────────────────
    _cd = cache_dir or os.path.join(os.path.dirname(out_path), "avatar_cache")
    all_uids = (
        ([nj_qq] if nj_qq else [])
        + [u["uid"] for u in ranking_by_ml]
        + [u["uid"] for u in ranking_by_count]
    )
    tasks   = {qq: _fetch_qq_avatar(qq, _cd) for qq in set(all_uids) if qq}
    results = {}
    if tasks:
        fetched = await asyncio.gather(*tasks.values(), return_exceptions=True)
        for qq, data in zip(tasks.keys(), fetched):
            results[qq] = data if isinstance(data, bytes) else None

    # 加载称号配置
    _titles_cfg = _load_titles(titles_path) if titles_path else {"ml_tiers": [], "count_tiers": []}

    img  = Image.new("RGB", (W, H), BG_PAGE)
    draw = ImageDraw.Draw(img)

    # ── Header ───────────────────────────────────────
    _grad_h(draw, 0, 0, W, HDR_H, ACCENT_LIGHT, DEEP)
    fn = _font(_FONT_BOLD, 20*S)
    _tc(draw, W // 2, (HDR_H - _th(draw, "A", fn)) // 2, "nj 反草战绩", fn, TEXT_LIGHT)

    # ── Profile（nj头像 + 名字 + 副标题）─────────────
    py = HDR_H
    _grad_v(draw, 0, py, W, py + PRO_H, (255, 248, 250), BG_CONT)
    draw.line([(0, py + PRO_H - 2*S), (W, py + PRO_H - 2*S)], fill=ACCENT_SOFT, width=2*S)

    AV = 64*S
    ax = W // 2 - AV // 2
    ay = py + 14*S
    _shadow(draw, ax, ay, ax + AV, ay + AV, 5*S)
    nj_av_data = results.get(nj_qq)
    if nj_av_data:
        _paste_avatar(img, nj_av_data, ax, ay, AV)
        draw.rectangle([ax, ay, ax + AV, ay + AV], outline=ACCENT, width=3*S)
    else:
        _grad_h(draw, ax, ay, ax + AV, ay + AV, ACCENT_LIGHT, ACCENT)
        draw.rectangle([ax, ay, ax + AV, ay + AV], outline=ACCENT, width=3*S)
        fn = _font(_FONT_BOLD, 26*S)
        ini = nj_name[0] if nj_name else "N"
        _tc(draw, ax + AV // 2, ay + AV // 2 - _th(draw, ini, fn) // 2, ini, fn, TEXT_LIGHT)

    fn_name = _font(_FONT_BOLD, 15*S)
    _tc(draw, W // 2, ay + AV + 8*S, nj_name, fn_name, ACCENT)

    fn_sub = _font(_FONT_REG, 11*S)
    sub = "近30天反草战绩（仅统计选择里面的记录）"
    _tc(draw, W // 2, ay + AV + 8*S + _th(draw, nj_name, fn_name) + 6*S, sub, fn_sub, TEXT_GRAY)

    # ── 两列排行 ─────────────────────────────────────
    body_y  = py + PRO_H
    left_w  = HALF_W - GAP // 2
    right_w = W - HALF_W - GAP // 2

    # 中间分割线
    draw.rectangle(
        [HALF_W - GAP // 2, body_y, HALF_W + GAP // 2, body_y + BODY_H],
        fill=SEP,
    )

    _draw_ranking_section(
        img, draw, results, S,
        body_y, left_w, 0,
        "注入总量榜", ranking_by_ml, True,
        BODY_H, _titles_cfg,
    )
    _draw_ranking_section(
        img, draw, results, S,
        body_y, right_w, HALF_W + GAP // 2,
        "被注入次数榜", ranking_by_count, False,
        BODY_H, _titles_cfg,
    )

    # ── Footer ───────────────────────────────────────
    fy = H - FOOT_H
    draw.rectangle([0, fy, W, H], fill=BG_CONT)
    fn = _font(_FONT_REG, 11*S)
    _tc(draw, W // 2, fy + (FOOT_H - _th(draw, "A", fn)) // 2,
        "统计范围：近30天 · 数据每日滚动更新", fn, ACCENT)

    img.save(out_path, "PNG", optimize=True, compress_level=6)
    return out_path