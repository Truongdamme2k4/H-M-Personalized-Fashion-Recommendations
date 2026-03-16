import os
from PIL import Image
import concurrent.futures
from pathlib import Path
import time

# ====================================================
# 1. CẤU HÌNH ĐƯỜNG DẪN & KÍCH THƯỚC
# ====================================================
# Thư mục chứa 105k ảnh gốc (H&M thường chia thành các folder con 01, 02...)
INPUT_DIR = "./data/raw/images" 
# Thư mục chứa ảnh sau khi đã ép size
OUTPUT_DIR = "./data/processed/images_resized" 

# Kích thước chuẩn xác của mô hình ResNet50
TARGET_SIZE = (224, 224) 

def process_image(img_path):
    """Hàm xử lý cho từng bức ảnh độc lập"""
    try:
        # Lấy tên file gốc (VD: 0663713001.jpg)
        filename = os.path.basename(img_path)
        out_path = os.path.join(OUTPUT_DIR, filename)

        # Bỏ qua nếu ảnh đã được xử lý từ lần chạy trước (chống chạy lại từ đầu nếu máy lỡ tắt)
        if os.path.exists(out_path):
            return True 

        # Mở ảnh, xử lý và lưu lại
        with Image.open(img_path) as img:
            # Chuyển về hệ màu RGB (tránh lỗi crash với ảnh đen trắng hoặc RGBA)
            img = img.convert("RGB")
            # Ép size dùng thuật toán LANCZOS (giữ chi tiết tốt nhất cho AI)
            img = img.resize(TARGET_SIZE, Image.Resampling.LANCZOS)
            # Lưu ảnh với chất lượng 85% để giảm dung lượng
            img.save(out_path, "JPEG", quality=85)
            
        return True
    except Exception as e:
        print(f"❌ Lỗi ở ảnh {filename}: {str(e)}")
        return False

def main(test_mode=True):
    # Tạo thư mục đầu ra nếu chưa có
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"🔍 Đang quét toàn bộ ảnh trong: {INPUT_DIR}...")
    # Quét đệ quy tìm tất cả file .jpg (kể cả nằm trong thư mục con)
    all_images = list(Path(INPUT_DIR).rglob("*.jpg"))
    total_found = len(all_images)
    
    if total_found == 0:
        print("❌ Không tìm thấy file .jpg nào! Hãy kiểm tra lại đường dẫn INPUT_DIR.")
        return

    # ---------------------------------------------------------
    # CHẾ ĐỘ CHẠY (TEST / FULL)
    # ---------------------------------------------------------
    if test_mode:
        all_images = all_images[:100] # CHỈ LẤY 100 ẢNH ĐỂ TEST
        print(f"🛠️ [CHẾ ĐỘ TEST]: Bắt đầu ép size {len(all_images)} ảnh...")
    else:
        print(f"🚀 [CHẾ ĐỘ FULL]: Bắt đầu ép size TOÀN BỘ {len(all_images)} ảnh...")
        print("☕ Đi pha ly cà phê đi, việc này sẽ tốn chút thời gian đấy!")

    start_time = time.time()
    success_count = 0

    # ---------------------------------------------------------
    # KÍCH HOẠT ĐA LUỒNG (VẮT KIỆT CPU)
    # ---------------------------------------------------------
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # executor.map sẽ tự động chia việc cho các nhân CPU
        results = executor.map(process_image, all_images)
        for res in results:
            if res:
                success_count += 1

    end_time = time.time()
    minutes = (end_time - start_time) / 60
    
    print("\n" + "="*50)
    print(f"🎉 HOÀN TẤT!")
    print(f"✅ Đã xử lý thành công: {success_count} / {len(all_images)} ảnh")
    print(f"⏱️ Tổng thời gian chạy: {minutes:.2f} phút")
    print("="*50)

if __name__ == "__main__":
    # BƯỚC 1: ĐỂ True ĐỂ CHẠY THỬ 100 ẢNH.
    # BƯỚC 2: SAU KHI THẤY THƯ MỤC OUTPUT CÓ 100 ẢNH 224x224 XINH ĐẸP, ĐỔI THÀNH False ĐỂ CHẠY HẾT 105.000 ẢNH.
    main(test_mode=True)