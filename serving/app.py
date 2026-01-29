from xml.parsers.expat import model
import streamlit as st
import folium
from streamlit_folium import st_folium
import polyline
import requests
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from datetime import datetime

# --- Load ML model và Spark Session từ scripts/predict.py ---
import sys
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent  # serving
PROJECT_ROOT = BASE_DIR.parent
sys.path.append(str(PROJECT_ROOT))

from pyspark.ml import PipelineModel
from scripts.predict import get_spark_session, get_latest_model_folder, predict_single_record

@st.cache_resource
def load_model():
    print("--- Bắt đầu load model ---")
    try:
        print("1. Đang tìm model path từ S3...")
        model_path = get_latest_model_folder()
        print(f"   Path tìm thấy: {model_path}")

        if not model_path:
            st.error("Không tìm thấy model trên S3")
            return None, None
        
        print("2. Đang khởi tạo Spark...")
        spark = get_spark_session()

        print(f"3. Đang load model từ {model_path}...")
        spark_model = PipelineModel.load(model_path)
        
        print("--- Load thành công ---")
        return spark, spark_model
    except Exception as e:
        st.error(f"Không thể tải model từ {model_path}. Lỗi: {e}")
        return None, None
spark, spark_model = load_model()


# --- Load weather utility ---
from utils.get_weather_data import fetch_weather_info


# --- Khởi tạo Session State ---
# Đây là "bộ nhớ" giúp dữ liệu không bị mất khi map load lại
if "route_data" not in st.session_state:
    st.session_state.route_data = None

# --- Cấu hình trang ---
st.set_page_config(page_title="Taxi Travel Time Prediction")
st.title("🚖 Ước lượng thời gian di chuyển")

# --- Hàm hỗ trợ ---

# 1. Hàm chuyển tên địa điểm thành tọa độ (Geocoding)
def get_coordinates(address):
    geolocator = Nominatim(user_agent="bigdata_streamlit_app") # User_agent là bắt buộc
    try:
        location = geolocator.geocode(address)
        if location:
            return location.latitude, location.longitude
        else:
            return None, None
    except GeocoderTimedOut:
        return None, None

# 2. Hàm lấy đường đi từ OSRM (Open Source Routing Machine)
def get_osrm_route(start_lat, start_lon, end_lat, end_lon):
    url = f"http://router.project-osrm.org/route/v1/driving/{start_lon},{start_lat};{end_lon},{end_lat}?overview=full"
    try:
        response = requests.get(url)
        data = response.json()
        
        if data["code"] != "Ok":
            return None
        
        route = data["routes"][0]
        decoded = polyline.decode(route["geometry"])
        
        # --- PHẦN QUAN TRỌNG ĐÃ SỬA ---
        # Trả về Dictionary (có ngoặc nhọn {}) để code bên dưới đọc được data['dist']
        return {
            "path": decoded,
            "dist": route["distance"] / 1000, # Đổi ra km
            "dur": route["duration"] / 60,    # Đổi ra phút
            "start": [start_lat, start_lon],
            "end": [end_lat, end_lon]
        }
        # -----------------------------
    except Exception as e:
        st.error(f"Lỗi: {e}")
        return None
    
# 3. Hàm tính toán tác động thời tiết (Mới thêm)
def calculate_weather_impact(weather_data):
    """
    Phân tích thời tiết và đưa ra lời khuyên cho hành khách đi taxi.
    """
    if "error" in weather_data:
        return 0, ["⚠️ Không thể lấy dữ liệu thời tiết. Hãy cẩn thận khi di chuyển."], "gray"

    rain = weather_data.get('precipitation', 0.0)
    wind = weather_data.get('windspeed_10m', 0.0)
    temp = weather_data.get('temperature_2m', 0.0)
    
    delay_percent = 0.0
    advice_list = []
    status_color = "green" 

    # --- 1. XỬ LÝ MƯA (Ảnh hưởng lớn nhất) ---
    if rain > 7.6:
        delay_percent += 0.40
        advice_list.append(f"🌧️ **Mưa rất to ({rain}mm):** Tầm nhìn bị hạn chế và nguy cơ ngập nước.")
        advice_list.append("💡 **Lời khuyên:** Hãy đặt xe sớm hơn dự kiến. Chuyến đi có thể bị **chậm đáng kể** do tắc đường.")
        status_color = "red"
        
    elif rain > 2.5:
        delay_percent += 0.20
        advice_list.append(f"☔ **Mưa vừa ({rain}mm):** Đường khá trơn và các xe sẽ đi chậm lại.")
        advice_list.append("💡 **Lời khuyên:** Bạn nên trừ hao thêm thời gian. Chuyến đi có thể **chậm hơn bình thường**.")
        status_color = "orange"
        
    elif rain > 0.5:
        delay_percent += 0.05
        advice_list.append(f"🌦️ **Mưa nhỏ ({rain}mm):** Có mưa lất phất.")
        advice_list.append("💡 **Lời khuyên:** Mang theo ô dù khi xuống xe. Chuyến đi có thể **chậm một chút**.")
        if status_color == "green": status_color = "blue"

    # --- 2. XỬ LÝ GIÓ ---
    if wind > 50:
        delay_percent += 0.15
        advice_list.append(f"🌬️ **Gió mạnh ({wind}km/h):** Nếu lộ trình đi qua cầu cao hoặc đường thoáng, xe có thể phải giảm tốc.")
        if status_color != "red": status_color = "orange"

    # --- 3. XỬ LÝ NHIỆT ĐỘ KHẮC NGHIỆT ---
    if temp < 3.0:
        advice_list.append("💡 **Lời khuyên:** Trời lạnh, hãy đem theo áo ấm nhé.")

    if temp < 3.0 and rain > 0:
        delay_percent += 0.50
        advice_list.append("❄️ **CẢNH BÁO BĂNG GIÁ:** Trời lạnh buốt kèm mưa, mặt đường cực kỳ trơn trượt.")
        advice_list.append("🛑 **Cảnh báo:** Giao thông có thể tê liệt. Chỉ di chuyển khi thực sự cần thiết.")
        status_color = "red"

    # --- 4. TRƯỜNG HỢP TỐT ---
    if not advice_list:
        advice_list.append("☀️ **Thời tiết đẹp:** Trời khô ráo, tầm nhìn tốt.")
        advice_list.append("🚕 **Tín hiệu tốt:** Giao thông thuận lợi, khả năng cao bạn sẽ **đến đúng giờ**.")

    return delay_percent, advice_list, status_color

# --- Giao diện Sidebar ---
with st.sidebar:
    st.header("Nhập lộ trình")
    origin_input = st.text_input("Điểm đi", "Times Square (Manhattan, Midtown)")
    dest_input = st.text_input("Điểm đến", "Central Park, New York City")
    # Times Square (Manhattan, Midtown) / Central Park, New York City
    # John F. Kennedy International Airport/ Empire State Building, New York City
    # Brooklyn Bridge Park, New York City / Wall Street, New York City

    # Nút bấm chỉ dùng để kích hoạt tính toán
    submit = st.button("Tìm đường ngay")
    st.caption("Lưu ý: Nhập tên địa điểm cụ thể hoặc bằng Tiếng Anh/Tiếng Việt không dấu để chính xác hơn.")

    if submit:
        with st.spinner("Đang xử lý..."):
            s_lat, s_lon = get_coordinates(origin_input)
            e_lat, e_lon = get_coordinates(dest_input)
            
            if s_lat and e_lat:
                result = get_osrm_route(s_lat, s_lon, e_lat, e_lon)

                # Tính trung điểm để lấy thời tiết
                mid_lat = (s_lat + e_lat) / 2
                mid_lon = (s_lon + e_lon) / 2

                # Lấy thời tiết
                weather_data = fetch_weather_info(mid_lat, mid_lon, datetime.now())

                # Tính toán tác động
                delay_factor, warning_msgs, status_color = calculate_weather_impact(weather_data)

                # Dự đoán thời gian từ Model Spark
                predicted_duration = predict_single_record(
                    spark=spark,
                    model=spark_model,
                    pickup_time=datetime.now(),
                    trip_distance=result['dist'],
                    distance_unit="km",
                    midpoint_latitude=mid_lat,
                    midpoint_longitude=mid_lon,
                    temperature_2m=weather_data['temperature_2m'],
                    precipitation=weather_data['precipitation'],
                    windspeed_10m=weather_data['windspeed_10m'],
                    pressure_msl=weather_data['pressure_msl']
                )
                predicted_duration = predicted_duration/60  # Đổi ra giờ

                if result:
                    # LƯU KẾT QUẢ VÀO SESSION STATE
                    st.session_state.route_data = {
                        **result,
                        "weather": weather_data,
                        "warnings": warning_msgs,
                        "status_color": status_color,
                        "predicted_duration": predicted_duration,
                        "delay_factor": delay_factor
                    }
                else:
                    st.error("Không tìm thấy đường đi.")
            else:
                st.error("Không tìm thấy địa điểm.")

# --- Phần hiển thị (Nằm ngoài nút bấm) ---
# Kiểm tra nếu trong bộ nhớ có dữ liệu thì mới vẽ
if st.session_state.route_data:
    data = st.session_state.route_data
    w_data = data.get("weather", {})
    
    # Hiển thị thông số
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Khoảng cách", f"{data['dist']:.2f} km")
    c2.metric("Thời gian OSM", f"{data['dur']:.0f} phút")
    c3.metric("Thời gian Dự đoán", f"{predicted_duration:.0f} phút")

    # Hiển thị thời gian có tính mưa gió
    delta_time = data['predicted_duration'] / 60 * data['delay_factor']
    c4.metric(
        "Thời gian trễ do thời tiết", 
        f"+{delta_time:.0f} phút",
        delta_color="inverse"
    )

    # 2. Hiển thị Cảnh báo Thời tiết
    st.markdown("### 🌤️ Điều kiện thời tiết")
    
    # Dùng st.info, st.warning hoặc st.error tùy theo mức độ
    if data['status_color'] == 'green':
        st.success(data['warnings'][0])
    elif data['status_color'] == 'orange':
        st.warning("\n".join(data['warnings']))
    else:
        st.error("\n".join(data['warnings']))

    # Hiển thị chi tiết nhỏ bên dưới
    cols = st.columns(4)
    cols[0].caption(f"🌡️ Nhiệt độ: {w_data.get('temperature_2m', 0)} °C")
    cols[1].caption(f"💧 Lượng mưa: {w_data.get('precipitation', 0)} mm")
    cols[2].caption(f"💨 Tốc độ gió: {w_data.get('windspeed_10m', 0)} km/h")
    cols[3].caption(f"Độ trễ chuyến: +{data['delay_factor']*100:.0f}% time")

    # st.divider()

    # Vẽ Map
    mid_lat = (data['start'][0] + data['end'][0]) / 2
    mid_lon = (data['start'][1] + data['end'][1]) / 2
    
    m = folium.Map(location=[mid_lat, mid_lon], zoom_start=12)
    
    folium.Marker(data['start'], popup="Start", icon=folium.Icon(color="green", icon="play")).add_to(m)
    folium.Marker(data['end'], popup="End", icon=folium.Icon(color="red", icon="stop")).add_to(m)
    folium.PolyLine(data['path'], color="blue", weight=5).add_to(m)
    m.fit_bounds([data['start'], data['end']])

    # Quan trọng: returned_objects=[] giúp giảm bớt data gửi lại server, tránh lag
    st_folium(m, width=1000, height=500, returned_objects=[])