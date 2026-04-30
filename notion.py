#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
auto-zalo-v4.0.py
Cải tiến từ v3.1:
- Giữ nguyên toàn bộ logic Lịch G (fetch_pairs_from_notion, tìm tên, gửi tin)
- Thêm hỗ trợ Tổng lãi NG: fetch_pairs_from_ng, gộp kỳ cùng khách, mark đã nhắc
- Merge 2 nguồn trước khi sort/preview
"""

import os, time, re, random, traceback, requests
from datetime import date
from pathlib import Path
from typing import List, Tuple, Optional
from difflib import SequenceMatcher
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from datetime import timedelta
from dotenv import load_dotenv
# ========== CONFIG ==========
load_dotenv()
TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN",    "")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID",  "")
NOTION_TOKEN      = os.getenv("NOTION_TOKEN",      "")

# ── Lịch G (cũ – giữ nguyên) ──
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "")
NOTION_PROP_NAME   = "auto"

# ── Tổng lãi NG (mới) ──
# ⚠️ Thay bằng ID thật của database Tổng lãi NG trên Notion
NOTION_NG_DB_ID = os.getenv("NOTION_NG_DB_ID", "")

PROFILE_DIR        = Path.home() / ".config/google-chrome"
ZALO_URL           = "https://chat.zalo.me/"
PAGE_LOAD_TIMEOUT  = 60000
INPUT_WAIT_SEC     = 30
TELEGRAM_REPLY_WAIT = 600  # seconds

BASE_TELE_URL  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
NOTION_HEADERS = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type":   "application/json",
}

# ========== TELEGRAM ==========
def send_telegram(text: str):
    try:
        return requests.post(
            f"{BASE_TELE_URL}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=15,
        ).json()
    except Exception as e:
        print("send_telegram error:", e)
        return None

def edit_telegram_message(msg_id: int, text: str):
    try:
        requests.post(
            f"{BASE_TELE_URL}/editMessageText",
            json={"chat_id": TELEGRAM_CHAT_ID, "message_id": msg_id, "text": text},
            timeout=10,
        )
    except Exception as e:
        print("edit msg error:", e)

def chunk_and_send(header: str, lines: List[str], chunk_size: int = 3000):
    cur = header or ""
    for ln in lines:
        add = ln + "\n"
        if len(cur) + len(add) > chunk_size:
            send_telegram(cur.strip())
            cur = ""
        cur += add
    if cur.strip():
        send_telegram(cur.strip())

def get_updates(offset=None, timeout=10):
    try:
        params = {"timeout": timeout}
        if offset:
            params["offset"] = offset
        r = requests.get(f"{BASE_TELE_URL}/getUpdates", params=params, timeout=timeout + 10)
        return r.json().get("result", [])
    except Exception as e:
        return []

# ========== NOTION HELPERS (dùng chung) ==========
def _join_plain_text_array(arr):
    return (
        "".join([x.get("plain_text", "") for x in arr if isinstance(x, dict)]).strip()
        if arr else ""
    )

def extract_prop_value(props: dict, key: str) -> str:
    """Đọc giá trị prop theo tên (case-insensitive). Hỗ trợ title / rich_text / formula."""
    for k in props.keys():
        if k.lower() == key.lower():
            p = props[k]
            t = p.get("type")
            if t == "title":     return _join_plain_text_array(p.get("title", []))
            if t == "rich_text": return _join_plain_text_array(p.get("rich_text", []))
            if t == "formula":   return (p.get("formula", {}).get("string") or "").strip()
    return ""

# ========== LỊCH G – GIỮ NGUYÊN TOÀN BỘ ==========
def fetch_pairs_from_notion() -> List[Tuple[Optional[str], str]]:
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"

    # Retry 3 lần, mỗi lần cách 5s
    for attempt in range(3):
        try:
            r = requests.post(
                url, headers=NOTION_HEADERS,
                json={"page_size": 200},
                timeout=120,
            )
            if r.status_code != 200 or not r.text.strip():
                print(f"[LichG] Lần {attempt+1}: status={r.status_code} | body rỗng, thử lại...")
                time.sleep(5)
                continue
            data = r.json()
            break  # thành công
        except Exception as e:
            print(f"[LichG] Lần {attempt+1} lỗi: {e}, thử lại sau 5s...")
            time.sleep(5)
    else:
        print("[LichG] Thất bại sau 3 lần — trả về rỗng")
        return []

    pairs = []
    for p in data.get("results", []):
        raw = extract_prop_value(p.get("properties", {}), NOTION_PROP_NAME).strip()
        if not raw:
            continue
        if ":" in raw:
            n, m = raw.split(":", 1)
            pairs.append((n.strip(), m.strip()))
        else:
            pairs.append((None, raw))

    print(f"[LichG] ✅ Lấy được {len(pairs)} rows")
    return pairs

# ========== TỔNG LÃI NG – MỚI ==========
def _get_zalo_rollup(props: dict) -> str:
    """
    Đọc số Zalo từ cột Rollup trong Tổng lãi NG.
    Thử cả phone_number lẫn rich_text (tuỳ kiểu cột Zalo trong Lịch NG).
    """
    zalo_prop  = props.get("Zalo", {})
    rollup_arr = zalo_prop.get("rollup", {}).get("array", [])
    if not rollup_arr:
        return ""
    first = rollup_arr[0]
    # Kiểu Phone
    if first.get("type") == "phone_number":
        return first.get("phone_number", "").strip()
    # Kiểu Text / rich_text
    if first.get("type") == "rich_text":
        rt = first.get("rich_text", [])
        return rt[0].get("plain_text", "").strip() if rt else ""
    return ""

def _build_ng_msg(asset: str, asset_full: str, items: list) -> str:
    today    = date.today()
    total    = sum(i["amount"] for i in items)
    n_ky     = len(items)
    ky_str = f"{n_ky} kỳ" if n_ky >= 2 else ""

    # Delta tính từ kỳ MỚI NHẤT (due lớn nhất)
    latest   = max(i["due"] for i in items)
    delta    = (today - latest).days   # âm = chưa tới, 0 = hôm nay, dương = trễ

    # Ngày hiển thị = kỳ cũ nhất (để khách biết nợ từ bao giờ)
    earliest = min(i["due"] for i in items)

    tien_str = f"{int(total):,}"

    if delta < 0:
        days_left = abs(delta)
        suffix = f" | {ky_str} CK e" if ky_str else "  nha e"
        ky_note = f"\n⏰ {days_left} hôm nữa tới ngày{suffix}"
    elif delta == 0:
        suffix = f" | {ky_str} CK e" if ky_str else " CK a nha e"
        ky_note = f"\n⏰ Hôm nay tới ngày{suffix}"
    else:
        prefix = f"{ky_str} trễ" if ky_str else "Trễ"
        ky_note = f"\n🆘 {prefix} {delta} ngày rồi CK cho a"

    return (
        f"💰 : {asset_full}\n"
        f"📅 ngày: {earliest.isoformat()}\n"
        f"💵 tiền: {tien_str}"
        f"{ky_note}"
    )

def fetch_pairs_from_ng() -> List[Tuple[Optional[str], str, List[str]]]:
    """
    Query Tổng lãi NG: lấy tất cả kỳ Chưa thu, group theo khách,
    trigger nhắc nếu bất kỳ kỳ nào: còn 2 ngày, đúng ngày, trễ 3 hoặc 5 ngày.
    Khi trigger → kéo theo TẤT CẢ kỳ của khách đó vào 1 tin.
    """
    if NOTION_NG_DB_ID == "PASTE_TONG_LAI_NG_DB_ID_HERE":
        print("[WARN] NOTION_NG_DB_ID chưa được cấu hình — bỏ qua NG.")
        return []

    today     = date.today()
    today_str = today.isoformat()
    in2_str   = (today + timedelta(days=2)).isoformat()

    # ── Bước 1: Lấy TẤT CẢ kỳ Chưa thu ──
    url = f"https://api.notion.com/v1/databases/{NOTION_NG_DB_ID}/query"
    r   = requests.post(url, headers=NOTION_HEADERS, json={
        "page_size": 200,
        "filter": {"property": "Trạng thái", "select": {"equals": "Chưa thu"}},
    }, timeout=60)
    rows = r.json().get("results", [])
    print(f"[NG] Tổng rows Chưa thu: {len(rows)}")

    # ── Bước 2: Group tất cả kỳ theo asset ──
    groups: dict = {}
    for p in rows:
        props      = p.get("properties", {})
        title      = extract_prop_value(props, "Name").strip()
        asset      = title.split()[0].strip()
        asset_full = title.split("|")[0].strip()
        due_raw    = props.get("Ngày phải thu", {}).get("date", {}).get("start", "")
        amount     = props.get("Số tiền phải thu", {}).get("number", 0) or 0
        zalo       = _get_zalo_rollup(props)

        if not zalo or not due_raw or not asset:
            print(f"[NG] Bỏ qua: asset={asset!r} due={due_raw!r} zalo={zalo!r}")
            continue

        if asset not in groups:
            groups[asset] = {
                "zalo":       zalo,
                "asset_full": asset_full,
                "items":      [],
                "page_ids":   [],
            }
        groups[asset]["items"].append({
            "due":    date.fromisoformat(due_raw),
            "amount": amount,
        })
        groups[asset]["page_ids"].append(p["id"])

    # ── Bước 3: Kiểm tra trigger từng nhóm ──
    results = []
    for asset, g in groups.items():
        items  = g["items"]
        latest = max(i["due"] for i in items)

        # Delta của tất cả kỳ trong nhóm
        all_deltas = [(today - i["due"]).days for i in items]

        # Trigger nếu bất kỳ kỳ nào thỏa điều kiện
        trigger = (
            latest.isoformat() == in2_str       # kỳ muộn nhất còn 2 ngày
            or latest.isoformat() == today_str  # kỳ muộn nhất đúng hôm nay
            or any(d in (3, 5) for d in all_deltas)  # bất kỳ kỳ nào trễ đúng 3 hoặc 5 ngày
        )

        if not trigger:
            continue

        msg = _build_ng_msg(asset, g["asset_full"], items)
        results.append((g["zalo"], msg, g["page_ids"]))
        print(f"[NG] ✅ Nhắc: {asset} | {len(items)} kỳ | deltas={all_deltas}")

    print(f"[NG] Tổng khách cần nhắc: {len(results)}")
    return results

def mark_ng_reminded(page_ids: List[str]) -> None:
    """Cập nhật Đã nhắc = 'Đã' sau khi gửi tin thành công — tránh nhắc lại ngày mai."""
    for pid in page_ids:
        try:
            requests.patch(
                f"https://api.notion.com/v1/pages/{pid}",
                headers=NOTION_HEADERS,
                json={"properties": {"Đã nhắc": {"select": {"name": "Đã"}}}},
                timeout=30,
            )
        except Exception as e:
            print(f"[WARN] mark_ng_reminded lỗi {pid}: {e}")

# ========== PARSER ==========
def parse_selection_to_exclude(sel_text: str, total: int):
    if not sel_text:
        return []
    s = sel_text.strip().lower()
    if s in ("all", "/all", "tất cả", "tat ca"):
        return []
    parts = [p.strip() for p in re.split(r"[,\s]+", s) if p.strip()]
    out = set()
    for p in parts:
        try:
            if "-" in p:
                nums = re.findall(r"\d+", p)
                if len(nums) >= 2:
                    a, b = int(nums[0]), int(nums[1])
                    for i in range(min(a, b), max(a, b) + 1):
                        if 1 <= i <= total:
                            out.add(i)
                continue
            num_str = re.sub(r"\D", "", p)
            if num_str:
                i = int(num_str)
                if 1 <= i <= total:
                    out.add(i)
        except Exception:
            continue
    return sorted(list(out))

def extract_day_sort(content: str) -> int:
    """Lấy số ngày từ chuỗi (vd: '4 ngày' => 4). Không tìm thấy => 9999."""
    try:
        m = re.search(r"(\d+)\s*ngày", (content or "").lower())
        return int(m.group(1)) if m else 9999
    except Exception:
        return 9999

def is_urgent(text: str) -> bool:
    return "🆘" in (text or "")

# ========== PLAYWRIGHT HELPERS – GIỮ NGUYÊN TOÀN BỘ ==========
def human_type_delayed(page, text: str, min_delay=0.06, max_delay=0.14):
    for ch in text:
        page.keyboard.insert_text(ch)
        time.sleep(random.uniform(min_delay, max_delay))

def try_focus_search_input(page, timeout_ms=3000) -> Optional[str]:
    selectors = [
        'input[aria-label*="Tìm"]',
        'input[placeholder*="Tìm"]',
        'input[placeholder*="tìm"]',
        'input[type="search"]',
        'input[role="searchbox"]',
    ]
    for sel in selectors:
        try:
            page.wait_for_selector(sel, timeout=timeout_ms)
            return sel
        except Exception:
            continue
    try:
        els = page.query_selector_all("input,textarea")
        for el in els:
            try:
                r = el.bounding_box()
                if not r:
                    continue
                if r["width"] > 40 and r["height"] > 12:
                    return None
            except:
                continue
    except:
        pass
    return None

def focus_search_input_dom(page):
    sel = try_focus_search_input(page, timeout_ms=2000)
    if sel:
        try:
            page.click(sel, timeout=1000)
            page.fill(sel, "")
            return True
        except:
            pass
    inputs = page.query_selector_all("input,textarea")
    for el in inputs:
        try:
            box = el.bounding_box()
            if box and box["width"] > 50 and box["height"] > 20:
                el.click()
                el.fill("")
                return True
        except:
            pass
    return False

def set_input_value_dispatch(page, selector: Optional[str], value: str):
    try:
        if selector:
            try:
                page.click(selector, timeout=2000)
            except Exception:
                pass
            page.fill(selector, "")
            time.sleep(0.3)
            page.evaluate(
                """(sel,val)=>{
                    const el=document.querySelector(sel);
                    if(el){
                        el.focus();
                        el.value=val;
                        el.dispatchEvent(new Event('input',{bubbles:true}));
                    }
                }""",
                selector, value,
            )
            time.sleep(random.uniform(1.2, 2.0))
            return True

        inputs = page.query_selector_all("input,textarea")
        for el in inputs:
            try:
                box = el.bounding_box()
                if not box or box["width"] < 50 or box["height"] < 20:
                    continue
                el.click()
                el.fill("")
                page.evaluate(
                    """(e,val)=>{
                        e.focus();
                        e.value=val;
                        e.dispatchEvent(new Event('input',{bubbles:true}));
                    }""",
                    el, value,
                )
                time.sleep(random.uniform(1.2, 2.0))
                return True
            except Exception:
                continue
    except Exception as e:
        print("set_input_value_dispatch error:", e)
    return False

def extract_id_from_name(name: str) -> str:
    """Trích xuất ID từ tên khách, ví dụ G001 từ G001-linh12pr-0867-ic99"""
    if name:
        match = re.match(r"^(G\d+)-", name)
        if match:
            return match.group(1)
    return ""

def find_and_click_best_match(page, target_name: str, wait_ms=1000) -> bool:
    target    = target_name.strip().lower()
    target_id = extract_id_from_name(target_name).lower()
    candidate_items = [
        'div[role="listitem"]',
        'div[class*="list-item"]',
        'div[role="option"]',
        'li',
        'div[aria-label*="Trò chuyện"]',
        'div[class*="contact"]',
    ]

    candidates = []
    first_el   = None
    end_time   = time.time() + wait_ms / 1000.0

    while time.time() < end_time:
        for sel in candidate_items:
            try:
                els = page.query_selector_all(sel)
                if not els:
                    continue
                for el in els:
                    try:
                        text = el.inner_text().strip().lower()
                        if text:
                            if not first_el:
                                first_el = el
                            norm_text   = re.sub(r"\s+", " ", re.sub(r"[^\w\s]", "", text))
                            norm_target = re.sub(r"\s+", " ", re.sub(r"[^\w\s]", "", target))
                            similarity  = SequenceMatcher(None, norm_text, norm_target).ratio()
                            candidates.append((similarity, el))
                    except Exception:
                        continue
            except Exception:
                continue
        time.sleep(0.35)

    if first_el:
        try:
            first_el.click(timeout=2000)
            time.sleep(random.uniform(1.5, 2.5))
            return True
        except Exception as e:
            print("click first suggestion fail:", e)

    if candidates:
        norm_target = re.sub(r"\s+", " ", re.sub(r"[^\w\s]", "", target))
        for sim, el in candidates:
            norm_text = re.sub(r"\s+", " ", re.sub(r"[^\w\s]", "", el.inner_text().strip().lower()))
            if target_id and target_id in norm_text:
                try:
                    el.click(timeout=2000)
                    time.sleep(random.uniform(1.5, 2.5))
                    return True
                except Exception as e:
                    print("click ID match fail:", e)
            if norm_text == norm_target:
                try:
                    el.click(timeout=2000)
                    time.sleep(random.uniform(1.5, 2.5))
                    return True
                except Exception as e:
                    print("click exact match fail:", e)

        candidates.sort(key=lambda x: x[0], reverse=True)
        best_sim, best_el = candidates[0]
        if best_sim >= 0.8:
            try:
                best_el.click(timeout=2000)
                time.sleep(random.uniform(1.5, 2.5))
                return True
            except Exception as e:
                print("click best sim fail:", e)

    return False

# ============================================================
# ========== GỬI TIN ZALO – GIỮ NGUYÊN TOÀN BỘ ==============
# ============================================================
def send_messages_with_playwright(pairs: List[Tuple[Optional[str], str]]):
    results = {"sent": 0, "failed": []}
    if not pairs:
        return results

    with sync_playwright() as p:
        PROFILE_DIR.mkdir(parents=True, exist_ok=True)
        browser = p.chromium.launch_persistent_context(
            str(PROFILE_DIR),
            headless=False,
            executable_path="/usr/bin/google-chrome",
            args=["--no-sandbox", "--disable-dev-shm-usage", "--start-maximized"]
        )
        page = browser.new_page()
        try:
            page.goto(ZALO_URL, timeout=PAGE_LOAD_TIMEOUT)
            try:
                page.wait_for_load_state("networkidle", timeout=20000)
            except:
                time.sleep(2)
        except Exception as e:
            print("Failed to open Zalo:", e)
        time.sleep(2)

        try:
            qr = page.query_selector('canvas, img[alt*="QR"], div[class*="qr"]')
            if qr:
                # Chụp ảnh màn hình gửi về Telegram
                screenshot_path = "/root/zalo/qr_screenshot.png"
                page.screenshot(path=screenshot_path)
                
                # Gửi ảnh qua Telegram
                with open(screenshot_path, "rb") as f:
                    requests.post(
                        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
                        data={"chat_id": TELEGRAM_CHAT_ID, "caption": "⚠️ Zalo yêu cầu đăng nhập lại! Quét QR này."},
                        files={"photo": f},
                        timeout=15
                    )
                
                send_telegram("⚠️ Zalo cần đăng nhập lại — đã gửi ảnh QR. Sau khi quét xong gửi /done để tiếp tục.")
                
                # Chờ 5 phút để quét QR
                time.sleep(300)
        except Exception as e:
            print(f"[WARN] Kiểm tra QR lỗi: {e}")
            
        msg    = send_telegram(f"🟢 Bắt đầu gửi {len(pairs)} tin nhắn Zalo...")
        msg_id = msg.get("result", {}).get("message_id") if msg else None
        total  = len(pairs)

        for idx, (name, msgtext) in enumerate(pairs, start=1):
            name_display = name or "(không tên)"
            bar_blocks   = int((idx / total) * 10)
            progress     = "█" * bar_blocks + "░" * (10 - bar_blocks)
            percent      = int((idx / total) * 100)
            icon         = ["💫", "🔄", "🌀", "✨"][idx % 4]
            if msg_id:
                edit_telegram_message(
                    msg_id,
                    f"{icon} Đang gửi {idx}/{total} [{progress}] {percent}%\n👤 Khách: {name_display}",
                )
            time.sleep(random.uniform(1.5, 2.5))

            page.keyboard.press("Escape")
            time.sleep(0.3)
            
            try:
                # 1️⃣ focus search input
                sel = try_focus_search_input(page, timeout_ms=500)
                set_input_value_dispatch(page, sel, name or "")

                max_wait       = 1.0
                start_w        = time.time()
                suggest_loaded = False
                while time.time() - start_w < max_wait:
                    try:
                        items = page.query_selector_all(
                            'div[role="listitem"], div[class*="list-item"], div[role="option"], li'
                        )
                        if items and len(items) >= 1:
                            suggest_loaded = True
                            break
                    except:
                        pass
                    time.sleep(0.12)

                if not suggest_loaded:
                    print(f"[WARN] Zalo load chậm — không thấy suggestion nhưng vẫn tiếp tục...")
                else:
                    time.sleep(random.uniform(0.05, 0.12))

                # 2️⃣ click vào khách hàng
                clicked = False
                if name:
                    clicked = find_and_click_best_match(page, name, wait_ms=500)
                    if not clicked:
                        try:
                            page.keyboard.press("Escape")
                            time.sleep(0.2)
                            page.keyboard.down("Control")
                            page.keyboard.press("f")
                            page.keyboard.up("Control")
                            time.sleep(0.3)
                            for _ in range(3):
                                page.keyboard.press("Backspace")
                                time.sleep(0.05)
                            page.evaluate(
                                "(txt)=>{document.execCommand('insertText', false, txt)}", name
                            )
                            time.sleep(1.0)
                            page.keyboard.press("Enter")
                            time.sleep(1.0)
                            clicked = True
                        except:
                            clicked = False

                if name and not clicked:
                    raise RuntimeError("Không tìm thấy suggestion cho khách hàng")

                # 3️⃣ chờ khung chat
                # MỚI
                try:
                    page.wait_for_selector(
                        'div[contenteditable="true"]',
                        timeout=INPUT_WAIT_SEC * 500,
                        state="visible",
                    )
                    page.click('div[contenteditable="true"]')
                except PWTimeout:
                    time.sleep(5)   # chờ thêm 5 giây
                    page.wait_for_selector(
                        'div[contenteditable="true"]',
                        timeout=10000,   # ← tăng lên 10 giây
                        state="visible"
                    )
                    page.click('div[contenteditable="true"]')

                # 4️⃣ gõ & gửi
                human_type_delayed(page, msgtext, min_delay=0.04, max_delay=0.12)
                page.keyboard.press("Enter")
                time.sleep(random.uniform(0.5, 1.0))
                try:
                    page.wait_for_selector('div[class*="sent"]', timeout=500)
                except:
                    print("[WARN] Không tìm thấy indicator sent, tiếp tục...")

                results["sent"] += 1

            except Exception as e:
                print(f"Fail for {name_display}: {e}")
                results["failed"].append((idx, name_display, str(e)))
                continue
            # THAY BẰNG:
            finally:
                try:
                    page.keyboard.press("Escape")
                    time.sleep(0.5)
                    page.keyboard.press("Escape")
                    time.sleep(1.0)
                except:
                    pass

        final_text = f"✅ Hoàn tất gửi {results['sent']} / {total} tin 🎉"
        if msg_id:
            edit_telegram_message(msg_id, final_text)
        else:
            send_telegram(final_text)

        if results["failed"]:
            lines = ["⚠️ Một số mục gửi lỗi:"]
            for idx, name_display, err in results["failed"]:
                lines.append(f"{idx}. {name_display} -> {err}")
            chunk_and_send("", lines)

        try:
            browser.close()
        except:
            pass

    return results

# ========== PREVIEW ==========
def send_preview_and_instructions(sorted_pairs):
    total  = len(sorted_pairs)
    urgent = sum(1 for _, m in sorted_pairs if is_urgent(m))
    header = (
        f"🔎 Có {total} tin cần gửi.\n"
        f"⚠️ Trong đó {urgent} tin có 🆘\n\n"
    )
    lines  = [f"{i}. {n}\n{m}" for i, (n, m) in enumerate(sorted_pairs, start=1)]
    footer = "\n📤 Nhập số \n/all \n/cancel \n"
    lines.append(footer)
    chunk_and_send(header, lines)

# ========== MAIN ==========
def main():
    print("▶️ Start auto-zalo-v4.0.py ...")
    send_telegram("🔔 Hệ thống đang khởi động...")

    # ── 1. Query Lịch G (cũ – giữ nguyên) ──
    try:
        pairs = fetch_pairs_from_notion()
    except Exception as e:
        send_telegram(f"❌ Lỗi Lịch G: {e}")
        pairs = []

    # ── 2. Query Tổng lãi NG (mới) ──
    ng_raw: List[Tuple[Optional[str], str, List[str]]] = []
    try:
        ng_raw = fetch_pairs_from_ng()
        # Chuyển sang format (name, msg) cho phần sort/preview/gửi
        ng_pairs = [(zalo, msg) for zalo, msg, _ in ng_raw]
        pairs.extend(ng_pairs)
        if ng_pairs:
            send_telegram(f"📋 NG: {len(ng_pairs)} kỳ cần nhắc hôm nay.")
    except Exception as e:
        send_telegram(f"⚠️ Lỗi query NG: {e}")

    if not pairs:
        send_telegram("⚠️ Không có dữ liệu nào cần gửi hôm nay.")
        return

    # ── 3. Sort: bình thường theo ngày → 🆘 (giữ nguyên logic cũ) ──
    normal_items, urgent_items = [], []
    for n, m in pairs:
        if is_urgent(m):
            urgent_items.append((n, m))
        else:
            normal_items.append((n, m))

    normal_items.sort(key=lambda x: extract_day_sort(x[1]))
    urgent_items.sort(key=lambda x: extract_day_sort(x[1]))
    sorted_pairs = normal_items + urgent_items

    # ── 4. Preview & chờ xác nhận (giữ nguyên) ──
    send_preview_and_instructions(sorted_pairs)

    msg    = send_telegram(f"⏳ Đang chờ phản hồi... (0/{TELEGRAM_REPLY_WAIT}s)")
    msg_id = msg.get("result", {}).get("message_id") if msg else None
    frames = ["⏳", "🔄", "💫", "✨"]
    start  = time.time()
    user_reply = None
    offset     = None
    ups = get_updates(timeout=2)
    if ups:
        offset = max(u["update_id"] for u in ups) + 1

    while time.time() - start < TELEGRAM_REPLY_WAIT:
        elapsed = int(time.time() - start)
        frame   = frames[(elapsed // 2) % len(frames)]
        if msg_id:
            edit_telegram_message(
                msg_id, f"{frame} Đang chờ phản hồi... ({elapsed}s/{TELEGRAM_REPLY_WAIT}s)"
            )
        ups = get_updates(offset=offset, timeout=3)
        if ups:
            for u in ups:
                offset = u["update_id"] + 1
                m      = u.get("message") or u.get("edited_message")
                if not m:
                    continue
                if str(m["chat"]["id"]) != str(TELEGRAM_CHAT_ID):
                    continue
                txt = (m.get("text") or "").strip()
                if not txt:
                    continue
                if txt.lower() in ("/cancel", "cancel"):
                    if msg_id:
                        edit_telegram_message(msg_id, "🔄 Nhận /cancel — đang làm mới...")
                    return main()
                user_reply = txt
                break
        if user_reply:
            break
        time.sleep(1.5)

    if not user_reply:
        if msg_id:
            edit_telegram_message(msg_id, f"⏰ Hết {TELEGRAM_REPLY_WAIT}s — gửi tất cả.")
        excludes = []
    else:
        if msg_id:
            edit_telegram_message(msg_id, f"✅ Nhận phản hồi: {user_reply}")
        excludes = parse_selection_to_exclude(user_reply, len(sorted_pairs))
        preview  = []
        for idx, (n, m) in enumerate(sorted_pairs, start=1):
            if idx in excludes:
                preview.append(f"{idx}. {n}\n{m}   ❌ BỎ QUA")
            else:
                preview.append(f"{idx}. {n}\n{m}")

    send_list = [x for i, x in enumerate(sorted_pairs, start=1) if i not in excludes]
    send_telegram(f"📤 Bắt đầu gửi {len(send_list)} tin nhắn (loại trừ {len(excludes)}).")

    # ── 5. Gửi tin (giữ nguyên) ──
    results = send_messages_with_playwright(send_list)

    # ── 6. Mark NG đã nhắc (chỉ những tin NG được gửi thành công) ──
    if ng_raw:
        # Xác định index của NG trong sorted_pairs để biết cái nào được gửi
        g_count  = len(pairs) - len(ng_raw)   # số tin Lịch G
        sent_set = {i - 1 for i, x in enumerate(sorted_pairs, start=1) if i not in excludes}

        for ng_idx, (zalo, msg_text, page_ids) in enumerate(ng_raw):
            # Tìm vị trí của tin NG này trong sorted_pairs
            for sp_idx, (sp_name, sp_msg) in enumerate(sorted_pairs):
                if sp_name == zalo and sp_msg == msg_text and sp_idx in sent_set:
                    mark_ng_reminded(page_ids)
                    break

    # ── 7. Tổng hợp 🆘 (giữ nguyên) ──
    urgent_sent = [(n, m) for n, m in send_list if is_urgent(m)]
    if urgent_sent:
        text = "🆘 Tổng hợp tin khẩn:\n" + "\n".join(
            [f"{i}. {n}: {m}" for i, (n, m) in enumerate(urgent_sent, start=1)]
        )
        chunk_and_send("", [text])
    else:
        send_telegram("✅ Không có tin khẩn cần xử lý.")

    send_telegram("🟢 Quy trình hoàn tất 🎉")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("🛑 Dừng thủ công.")
    except Exception as e:
        traceback.print_exc()
        send_telegram(f"❌ Lỗi chính: {e}")
