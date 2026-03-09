"""
nj体内卡片 Pillow 渲染  —  对照 nj_body.html 还原
风格：白底 / 粉色 accent / 方形像素阴影 / 无圆角
async 版本，内部自动下载 QQ 头像
"""
from PIL import Image, ImageDraw, ImageFont
import sys, os, io
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


def _pick_title(tiers: list, key_min: str, value) -> list:
    """从分段配置中匹配档位，返回该档位的titles列表，无匹配返回空列表"""
    for tier in tiers:
        if value >= tier[key_min]:
            return [t for t in tier.get("titles", []) if t]
    return []


def _pick_combined_title(cfg: dict, ml_value: float, count_value: int) -> str | None:
    """合并ml和count各自匹配档位的titles池，随机抽一条返回"""
    import random
    pool = _pick_title(cfg.get("ml_tiers", []), "min_ml", ml_value) +            _pick_title(cfg.get("count_tiers", []), "min_count", count_value)
    return random.choice(pool) if pool else None

# CSS 变量对照
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
    bb = draw.textbbox((0,0), text, font=fnt); return bb[2]-bb[0]

def _th(draw, text, fnt):
    bb = draw.textbbox((0,0), text, font=fnt); return bb[3]-bb[1]

def _tc(draw, cx, y, text, fnt, fill):
    draw.text((cx - _tw(draw,text,fnt)//2, y), text, font=fnt, fill=fill)

def _tag(draw, x, y, text, fnt, cl, cr, s):
    PX, PY = 8*s, 4*s
    bw = _tw(draw, text, fnt) + PX*2
    bh = _th(draw, text, fnt) + PY*2
    _shadow(draw, x, y, x+bw, y+bh, 2*s)
    _grad_h(draw, x, y, x+bw, y+bh, cl, cr)
    draw.text((x+PX, y+PY), text, font=fnt, fill=TEXT_LIGHT)
    return bw, bh

def _paste_avatar(img, avatar_bytes, x, y, size):
    """将头像 bytes 贴到 img 的指定位置，失败静默跳过"""
    try:
        av = Image.open(io.BytesIO(avatar_bytes)).convert("RGB")
        av = av.resize((size, size), Image.LANCZOS)
        img.paste(av, (x, y))
    except Exception:
        pass

async def _fetch_qq_avatar(qq: str, cache_dir: str, size: int = 100) -> bytes | None:
    """下载 QQ 头像，优先读本地缓存，失败返回 None"""
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


async def render_nj_body(
    nj_qq, nj_name,
    total_ml_str, total_count,
    reset_date, days_left, hours_left,
    ranking,          # list of {uid, name, count, ml, _ml_raw}  ← ml 已是格式化字符串
    out_path,
    cache_dir=None,   # 头像缓存目录，None 则不缓存
    titles_path=None, # nj_body_titles.json 路径，None 则不显示称号
    scale=2,
):
    S = scale
    W = 420 * S

    HDR_H  = 54  * S
    PRO_H  = 130 * S
    STA_H  = 90  * S
    RES_H  = 52  * S
    RNK_H  = 38  * S if ranking else 0
    ITEM_H = 72  * S
    EMPT_H = 60  * S if not ranking else 0
    FOOT_H = 36  * S
    H = HDR_H + PRO_H + STA_H + RES_H + RNK_H + ITEM_H*len(ranking) + EMPT_H + FOOT_H

    # ── 并发下载所有头像 ──────────────────────────────
    import asyncio
    _cd = cache_dir or os.path.join(os.path.dirname(out_path), "avatar_cache")
    qq_list = ([nj_qq] if nj_qq else []) + [u["uid"] for u in ranking]
    tasks   = {qq: _fetch_qq_avatar(qq, _cd) for qq in set(qq_list) if qq}
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
    _tc(draw, W//2, (HDR_H - _th(draw,"A",fn))//2, "nj 体内液体报告", fn, TEXT_LIGHT)

    # ── Profile ──────────────────────────────────────
    py = HDR_H
    _grad_v(draw, 0, py, W, py+PRO_H, (255,248,250), BG_CONT)
    draw.line([(0, py+PRO_H-2*S), (W, py+PRO_H-2*S)], fill=ACCENT_SOFT, width=2*S)

    AV = 80*S; ax = W//2-AV//2; ay = py+18*S
    _shadow(draw, ax, ay, ax+AV, ay+AV, 6*S)
    nj_av_data = results.get(nj_qq)
    if nj_av_data:
        _paste_avatar(img, nj_av_data, ax, ay, AV)
        draw.rectangle([ax, ay, ax+AV, ay+AV], outline=ACCENT, width=3*S)
    else:
        _grad_h(draw, ax, ay, ax+AV, ay+AV, ACCENT_LIGHT, ACCENT)
        draw.rectangle([ax, ay, ax+AV, ay+AV], outline=ACCENT, width=3*S)
        fn = _font(_FONT_BOLD, 32*S)
        ini = nj_name[0] if nj_name else "N"
        _tc(draw, ax+AV//2, ay+AV//2-_th(draw,ini,fn)//2, ini, fn, TEXT_LIGHT)

    fn = _font(_FONT_BOLD, 16*S)
    _tc(draw, W//2, ay+AV+10*S, nj_name, fn, ACCENT)

    # ── Stats ────────────────────────────────────────
    sy = py + PRO_H
    draw.rectangle([0, sy, W, sy+STA_H], fill=(255,250,252))
    draw.line([(0, sy+STA_H-2*S), (W, sy+STA_H-2*S)], fill=SEP, width=2*S)
    CM = 20*S; CW = (W-CM*3)//2; CH = 66*S; boxy = sy+(STA_H-CH)//2

    def _sbox(x, y, w, h, val, lbl):
        _shadow(draw, x, y, x+w, y+h, 4*S)
        draw.rectangle([x, y, x+w, y+h], fill=BG_CONT)
        draw.rectangle([x, y, x+w, y+h], outline=ACCENT_LIGHT, width=2*S)
        fv = _font(_FONT_BOLD, 22*S); fl = _font(_FONT_REG, 11*S)
        vh = _th(draw, val, fv); lh2 = _th(draw, lbl, fl)
        vy = y + (h - vh - 6*S - lh2) // 2
        _tc(draw, x+w//2, vy,        val, fv, ACCENT)
        _tc(draw, x+w//2, vy+vh+6*S, lbl, fl, TEXT_GRAY)

    _sbox(CM,      boxy, CW, CH, str(total_count), "本月累计注入次数")
    _sbox(CM*2+CW, boxy, CW, CH, total_ml_str,     "体内总量")

    # ── Reset info ───────────────────────────────────
    ry = sy + STA_H
    draw.rectangle([0, ry, W, ry+RES_H], fill=BG_CONT)
    draw.line([(0, ry+RES_H-2*S), (W, ry+RES_H-2*S)], fill=SEP, width=2*S)
    fr  = _font(_FONT_REG,  12*S)
    frb = _font(_FONT_BOLD, 12*S)
    lh1 = _th(draw, "A", fr)
    tv  = ry + (RES_H - lh1*2 - 8*S) // 2
    _tc(draw, W//2, tv,          f"本月刷新时间：{reset_date}",               fr,  TEXT_GRAY)
    _tc(draw, W//2, tv+lh1+8*S, f"距下次刷新：{days_left}天{hours_left}小时", frb, ACCENT)

    # ── Ranking ──────────────────────────────────────
    ly = ry + RES_H
    if ranking:
        draw.rectangle([0, ly, W, ly+RNK_H], fill=BG_CONT)
        draw.line([(0, ly+RNK_H-2*S), (W, ly+RNK_H-2*S)], fill=SEP, width=2*S)
        fn = _font(_FONT_BOLD, 13*S)
        draw.text((15*S, ly+(RNK_H-_th(draw,"A",fn))//2),
                  "注入排行榜", font=fn, fill=ACCENT)
        ly += RNK_H

        ft = _font(_FONT_REG, 11*S)
        RANK_SZ = [20*S, 18*S, 18*S]
        RANK_CL = [RANK1, RANK2, RANK3]

        for i, user in enumerate(ranking):
            iy = ly + i*ITEM_H
            draw.rectangle([0, iy, W, iy+ITEM_H],
                           fill=BG_ALT if i%2==0 else BG_CONT)
            draw.line([(0, iy+ITEM_H-2*S), (W, iy+ITEM_H-2*S)],
                      fill=SEP, width=2*S)

            # 排名数字
            fn_rk = _font(_FONT_BOLD, RANK_SZ[i] if i<3 else 16*S)
            rc    = RANK_CL[i] if i<3 else ACCENT
            rnum  = str(i+1)
            draw.text((15*S + (30*S - _tw(draw,rnum,fn_rk))//2,
                       iy + (ITEM_H - _th(draw,rnum,fn_rk))//2),
                      rnum, font=fn_rk, fill=rc)

            # 头像
            AV2 = 42*S; avx = 55*S; avy = iy+(ITEM_H-AV2)//2
            _shadow(draw, avx, avy, avx+AV2, avy+AV2, 4*S)
            av_data = results.get(user["uid"])
            if av_data:
                _paste_avatar(img, av_data, avx, avy, AV2)
                draw.rectangle([avx, avy, avx+AV2, avy+AV2],
                               outline=ACCENT_LIGHT, width=2*S)
            else:
                _grad_h(draw, avx, avy, avx+AV2, avy+AV2, ACCENT_LIGHT, ACCENT)
                draw.rectangle([avx, avy, avx+AV2, avy+AV2],
                               outline=ACCENT_LIGHT, width=2*S)
                fn_i2 = _font(_FONT_BOLD, 14*S)
                ini2  = user["name"][0] if user["name"] else "?"
                _tc(draw, avx+AV2//2, avy+(AV2-_th(draw,ini2,fn_i2))//2,
                    ini2, fn_i2, TEXT_LIGHT)

            # 名字 + 称号标签（合并ml/count池随机抽一条，紫色）
            fn_nm  = _font(_FONT_MED, 14*S)
            ft_t   = _font(_FONT_REG,  9*S)
            nx     = avx + AV2 + 10*S
            title     = _pick_combined_title(_titles_cfg, user.get("_ml_raw", 0.0), user.get("count", 0))
            has_title = bool(title)

            if has_title:
                name_y = iy + ITEM_H//2 - _th(draw, user["name"], fn_nm) - 3*S
            else:
                name_y = iy + (ITEM_H - _th(draw, user["name"], fn_nm))//2
            draw.text((nx, name_y), user["name"], font=fn_nm, fill=TEXT_PRIMARY)

            if has_title:
                TAG_PX, TAG_PY = 5*S, 2*S
                tx = nx
                ty = name_y + _th(draw, user["name"], fn_nm) + 3*S
                bw = _tw(draw, title, ft_t) + TAG_PX*2
                bh = _th(draw, title, ft_t) + TAG_PY*2
                _shadow(draw, tx, ty, tx+bw, ty+bh, 2*S, (214, 190, 230))
                _grad_h(draw, tx, ty, tx+bw, ty+bh, (210,170,230), (150,80,160))
                draw.text((tx+TAG_PX, ty+TAG_PY), title, font=ft_t, fill=TEXT_LIGHT)

            # 双标签（右对齐，count上 ml下）
            cnt_txt = f"{user['count']} 次"
            ml_txt  = user["ml"]
            ft_h    = _th(draw, "A", ft)
            tag_h   = ft_h + 8*S
            tag_gap = 6*S
            cnt_w   = _tw(draw, cnt_txt, ft) + 16*S
            ml_w    = _tw(draw, ml_txt,  ft) + 16*S
            right   = W - 15*S
            ty0     = iy + (ITEM_H - tag_h*2 - tag_gap) // 2
            _tag(draw, right-cnt_w, ty0,               cnt_txt, ft, ACCENT_LIGHT, ACCENT, S)
            _tag(draw, right-ml_w,  ty0+tag_h+tag_gap,  ml_txt,  ft, ACCENT,       DEEP,   S)
    else:
        draw.rectangle([0, ly, W, ly+EMPT_H], fill=BG_CONT)
        fn = _font(_FONT_REG, 13*S)
        _tc(draw, W//2, ly+(EMPT_H-_th(draw,"A",fn))//2,
            "本月还没有人注入过哦~", fn, TEXT_GRAY)

    # ── Footer ───────────────────────────────────────
    fy = H - FOOT_H
    draw.rectangle([0, fy, W, H], fill=BG_CONT)
    fn = _font(_FONT_REG, 11*S)
    _tc(draw, W//2, fy+(FOOT_H-_th(draw,"A",fn))//2,
        "数据每月1日0点自动刷新", fn, ACCENT)

    img.save(out_path, "PNG", optimize=True, compress_level=6)
    return out_path