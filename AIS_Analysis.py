import os
import re
import glob
import yaml
import pandas as pd
from datetime import datetime
from pyais.stream import ByteStream

# ================= 配置区域 =================
INPUT_FOLDER = 
OUTPUT_FILE = 
CONFIG_FILE = 
# ===========================================

def load_config(config_path):
    if not os.path.exists(config_path):
        return {}
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

# === 映射函数===
def get_ship_detailed_type(code, config):
    if code is None: return "Unknown"
    try: c = int(code)
    except: return "Unknown"
    details = config.get('ship_type_details', {})
    if c in details: return details[c]
    range_key = f"{(c // 10) * 10}_range"
    return details.get(range_key, "未定义类型")

def get_visual_label(code, config):
    if code is None: return "other"
    try: c = int(code)
    except: return "other"
    mapping_rules = config.get('detection_mapping', [])
    for rule in mapping_rules:
        target_name = rule.get('name')
        if 'codes' in rule and c in rule['codes']: return target_name
        if 'ranges' in rule:
            for start, end in rule['ranges']:
                if start <= c <= end: return target_name
    return "other"

def get_nav_status_str(status_code, config):
    if status_code is None: return None
    status_map = config.get('nav_status', {})
    return status_map.get(status_code, f"未定义({status_code})")

def parse_filename_time(filepath):
    filename = os.path.basename(filepath)
    match = re.search(r'(\d{8})_(\d{6})', filename)
    if match:
        try:
            return datetime.strptime(f"{match.group(1)}{match.group(2)}", "%Y%m%d%H%M%S")
        except:
            return None
    return None

# === 核心逻辑：带状态的生成器 ===
class LogStreamContext:
    def __init__(self):
        self.current_line_time = None

def file_line_generator(filepath, context_obj):
    # 1. 确定基准时间（Fallback）
    fallback_time = parse_filename_time(filepath)
    if not fallback_time:
        fallback_time = datetime.fromtimestamp(os.path.getmtime(filepath))

    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            line = line.strip()
            if not line: continue

            # 2. 尝试解析行首时间
            # 格式: 2025-12-06 09:44:35.819 ...
            ts_match = re.search(r'^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}(?:\.\d+)?)', line)
            
            line_ts = None
            if ts_match:
                ts_str = ts_match.group(1)
                try:
                    if '.' in ts_str:
                        line_ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
                    else:
                        line_ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                except:
                    pass
            
            # 3. 更新时间策略
            if line_ts:
                context_obj.current_line_time = line_ts
                fallback_time = line_ts # 更新 fallback，防止下一行没时间时回跳
            else:
                context_obj.current_line_time = fallback_time

            # 4. 提取 NMEA 并 yield
            nmea_match = re.search(r'(!AI[A-Z]{3},.*?\*[0-9A-F]{2})', line)
            if nmea_match:
                yield nmea_match.group(1).encode('utf-8')

def parse_ais_final(input_dir, output_csv, config_path):
    config = load_config(config_path)
    all_files = glob.glob(os.path.join(input_dir, '*.log'))
    
    trajectory_points = []
    vessel_profile = {} 
    
    print(f"开始处理 {len(all_files)} 个文件 (Stream + 行级时间)...")

    for file_path in all_files:
        print(f"正在读取: {file_path}")
        
        # 初始化上下文容器
        time_ctx = LogStreamContext()
        
        # 创建生成器
        line_gen = file_line_generator(file_path, time_ctx)
        
        # 将生成器传给 ByteStream
        # ByteStream 会按需从 line_gen 拉取数据。
        # 当 ByteStream 吐出一个 msg 时，line_gen 刚好停在组成该 msg 的最后一行。
        # 此时 time_ctx.current_line_time 就是最后一行的接收时间。
        stream = ByteStream(line_gen)
        
        for msg in stream:
            try:
                # 获取该消息对应的时间（从上下文读取）
                exact_ts = time_ctx.current_line_time
                if not exact_ts: continue # 理论上不会发生

                decoded = msg.decode()
                
                # 兼容不同版本的 pyais
                if hasattr(decoded, 'to_dict'): data = decoded.to_dict()
                elif hasattr(decoded, 'asdict'): data = decoded.asdict()
                else: data = decoded.__dict__

                mmsi = data.get('mmsi')
                if not mmsi: continue

                # === 1. 动态信息 ===
                lat = data.get('lat') or data.get('latitude')
                lon = data.get('lon') or data.get('longitude')
                
                if lat is not None and lon is not None and lat <= 90:
                    trajectory_points.append({
                        'mmsi': mmsi,
                        'exact_time': exact_ts.strftime("%Y-%m-%d %H:%M:%S"),
                        'unix_timestamp': exact_ts.timestamp(),
                        'lat': lat,
                        'lon': lon,
                        'speed': data.get('speed') or data.get('sog'),
                        'course': data.get('course') or data.get('cog'),
                        'heading': data.get('heading') or data.get('true_heading'),
                        'status_code': data.get('status'),
                        'status_text': get_nav_status_str(data.get('status'), config)
                    })

                # === 2. 静态信息 ===
                st_code = data.get('ship_type') or data.get('shiptype')
                length, width = None, None
                tb, ts_dim = data.get('to_bow'), data.get('to_stern')
                tp, tsd = data.get('to_port'), data.get('to_starboard')
                if tb is not None and ts_dim is not None: length = tb + ts_dim
                if tp is not None and tsd is not None: width = tp + tsd

                if st_code is not None or length is not None:
                    if mmsi not in vessel_profile: vessel_profile[mmsi] = {}
                    if st_code is not None:
                        vessel_profile[mmsi]['ship_type_code'] = st_code
                        vessel_profile[mmsi]['ship_type_detail'] = get_ship_detailed_type(st_code, config)
                        vessel_profile[mmsi]['visual_label'] = get_visual_label(st_code, config)
                    if length is not None:
                        vessel_profile[mmsi]['length'] = length
                        vessel_profile[mmsi]['width'] = width
            
            except Exception:
                continue

    # === 合并输出 ===
    print(f"解析完成，正在合并 {len(trajectory_points)} 个轨迹点...")
    final_rows = []
    for p in trajectory_points:
        mmsi = p['mmsi']
        profile = vessel_profile.get(mmsi, {})
        
        merged = p.copy()
        merged.update({
            'ship_type_code': profile.get('ship_type_code'),
            'ship_type_detail': profile.get('ship_type_detail', 'Unknown'),
            'visual_label': profile.get('visual_label', 'other'),
            'length': profile.get('length'),
            'width': profile.get('width')
        })
        final_rows.append(merged)

    if not final_rows:
        print("没有提取到有效数据。")
        return

    df = pd.DataFrame(final_rows)
    cols_map = {
        'mmsi': 'MMSI', 'exact_time': '时间(Local)', 'unix_timestamp': 'Unix时间戳',
        'visual_label': '视觉类别', 'ship_type_detail': '原始类别描述',
        'ship_type_code': '原始类别编码', 'status_text': '航行状态',
        'lat': '纬度', 'lon': '经度', 'speed': '航速', 'course': '航向', 'heading': '船首向',
        'length': '船长', 'width': '船宽'
    }
    
    out_cols = list(cols_map.keys())
    for c in out_cols:
        if c not in df.columns: df[c] = None
    
    df = df[out_cols]
    df.rename(columns=cols_map, inplace=True)

    # 如果两条数据的 MMSI、时间戳、经纬度、航速、航向 完全一致，
    # 那么无论它前面有没有 [ERROR] 标签，它绝对是重复记录。
    
    original_count = len(df)
    
    # 关键去重字段
    subset_cols = [
        'MMSI', 
        'Unix时间戳', # 绝对时间
        '纬度', 
        '经度', 
        '航速', 
        '航向'
    ]
    
    # keep='first' 表示保留第一次出现的，删除后面重复的
    df.drop_duplicates(subset=subset_cols, keep='first', inplace=True)
    
    final_count = len(df)
    if original_count > final_count:
        print(f"⚠️ 检测到重复数据（可能是ERROR重试导致的），已剔除 {original_count - final_count} 条重复记录。")
    else:
        print("✅ 未检测到重复数据，数据源很干净。")

    # ==========================================
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"处理成功！结果已保存至 {output_csv}")

if __name__ == "__main__":
    if os.path.exists(INPUT_FOLDER) and os.path.exists(CONFIG_FILE):
        parse_ais_final(INPUT_FOLDER, OUTPUT_FILE, CONFIG_FILE)
    else:
        print("文件路径或配置文件不存在")