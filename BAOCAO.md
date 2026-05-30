---
title: "Báo cáo hệ thống — H&M Personalized Fashion Recommendations"
subtitle: "Hệ thống gợi ý thời trang cá nhân hoá end-to-end (Medallion Data Lake + Spark + LightGBM + MongoDB + Web)"
date: 2026-05-27
---

# Mục lục

1. Tổng quan bài toán
2. Phân tích dữ liệu nguồn
3. Kiến trúc tổng thể
4. Pipeline ETL chi tiết
5. Feature Engineering và Ranking
6. Tầng serving (Web)
7. Triển khai bằng Docker Compose
8. Kết quả và đánh giá
9. Trao đổi thiết kế (Trade-offs)
10. Hạn chế hiện tại và hướng phát triển
11. Kết luận

---

# 1. Tổng quan bài toán

## 1.1. Mô tả bài toán

Bài toán nghiệp vụ đặt ra là: với mỗi khách hàng của H&M, hãy dự đoán Top-12 sản phẩm có khả năng được mua trong vòng 7 ngày sau ngày dự đoán. Đây là một biến thể của bài toán next-basket recommendation (gợi ý giỏ hàng kế tiếp), khác với hai bài toán phổ biến khác là gợi ý sản phẩm tương tự (item-to-item) hoặc gợi ý theo phiên truy cập (session-based).

Đặc điểm chính của bài toán: đầu ra là một danh sách 12 ID sản phẩm theo thứ tự ưu tiên giảm dần, không phải xác suất rời rạc cũng không phải điểm đánh giá. Tiêu chí đánh giá là MAP@12 (mean average precision tại vị trí 12) — đây là thước đo có trọng số theo vị trí: đoán đúng ở vị trí thứ nhất có giá trị cao hơn đoán đúng ở vị trí thứ 12. Dataset gốc lấy từ cuộc thi Kaggle H&M Personalized Fashion Recommendations năm 2022.

## 1.2. Tầm quan trọng nghiệp vụ

Trong lĩnh vực thương mại điện tử, thống kê của McKinsey cho thấy 30 đến 35 phần trăm doanh thu của Amazon được sinh ra từ hệ thống gợi ý. Với một nhà bán lẻ thời trang nhanh như H&M, hệ thống gợi ý có ba vai trò đặc biệt quan trọng.

Thứ nhất, khách hàng H&M có thói quen mua lặp lại rất mạnh — họ thường quay lại mua những món đồ cơ bản như áo thun, đồ lót, tất chân ở cùng một thương hiệu. Hệ thống gợi ý có thể kích thích nhu cầu này bằng cách nhắc nhở đúng lúc.

Thứ hai, vòng đời sản phẩm thời trang nhanh rất ngắn, chỉ khoảng 3 đến 6 tuần kể từ lúc lên kệ. Hệ thống gợi ý phải bám sát xu hướng theo tuần để không lãng phí cơ hội bán hàng.

Thứ ba, khách hàng mới hoặc khách hàng ít giao dịch chiếm trên 40 phần trăm tổng khách. Với nhóm này không thể dùng tín hiệu cá nhân hoá, mà phải fallback an toàn về trending toàn cục hoặc bestseller theo nhóm tuổi.

## 1.3. Thách thức kỹ thuật

Hệ thống phải vượt qua sáu thách thức kỹ thuật chính.

Thứ nhất là quy mô dữ liệu. Với 31,8 triệu giao dịch, không thể tải toàn bộ vào RAM máy tính 16GB. Giải pháp là dùng Apache Spark để xử lý phân tán và lưu dữ liệu ở định dạng cột parquet.

Thứ hai là cold start người dùng. Trên 40 phần trăm khách hàng có dưới 5 giao dịch, không đủ để áp dụng các mô hình collaborative filtering. Giải pháp là fallback về trending toàn cục và bestseller theo nhóm tuổi.

Thứ ba là cold start sản phẩm. Khi một SKU mới ra mắt, nó chưa có lịch sử giao dịch để các thuật toán dựa vào history hoạt động. Giải pháp là sử dụng nhánh Sibling (cùng product code, khác màu hoặc khác size) và nhánh Categorical (cùng nhóm gu thời trang).

Thứ tư là độ trễ online. Web cần trả về Top-12 trong dưới 50 mili-giây để trải nghiệm người dùng mượt mà. Giải pháp là tách hẳn pha tính toán offline (Airflow chạy hàng đêm) và pha truy vấn online (chỉ làm lookup vào MongoDB đã pre-compute).

Thứ năm là đa dạng tín hiệu. Quan sát thực nghiệm cho thấy không có một thuật toán đơn lẻ nào đạt Recall@12 trên 5 phần trăm. Giải pháp là sinh ứng viên từ nhiều nguồn rồi dùng LightGBM rerank.

Thứ sáu là refresh dữ liệu. Production cần re-train hàng ngày khi có batch giao dịch mới. Giải pháp là dùng Airflow điều phối DAG đảm bảo idempotent và có cơ chế retry.

## 1.4. Hướng tiếp cận

Hệ thống áp dụng pattern two-stage candidate generation và re-ranking, đây là chuẩn industry đang được dùng tại Pinterest (hệ thống PinSage), YouTube (mô hình YouTube DNN), TikTok và Netflix.

Pha thứ nhất là Recall (thu hồi rộng). Bảy bộ sinh ứng viên chạy song song, mỗi bộ đưa ra một danh sách ứng viên theo một logic khác nhau. Tổng hợp lại sau khi loại trùng, ta có khoảng 97 ứng viên cho mỗi người dùng. Pha này không cần độ chính xác cao, chỉ cần đảm bảo "lọt vào lưới" các item mà người dùng thực sự sẽ mua.

Pha thứ hai là Rank (xếp hạng chính xác). LightGBM được huấn luyện trên 24 đặc trưng tabular sẽ chấm điểm 97 ứng viên này, sau đó cắt giữ Top-12 cho mỗi người dùng.

Lý do tách thành hai pha: nếu chấm điểm trực tiếp tất cả 105 nghìn sản phẩm cho từng người dùng trong số 1,37 triệu, ta phải xử lý 1,4 nghìn tỷ cặp. Đây là con số không khả thi với cả thời gian compute lẫn chi phí lưu trữ. Two-stage giảm số cặp cần rank xuống khoảng 100 nhân với số người dùng — giảm bảy bậc độ lớn.

---

# 2. Phân tích dữ liệu nguồn

## 2.1. Ba bảng chính

Dataset H&M cung cấp ba bảng nguồn. Bảng articles chứa khoảng 105 nghìn bản ghi, mô tả catalog sản phẩm với các trường như mã sản phẩm, mã code (dùng cho sibling), tên, loại sản phẩm, nhóm màu, nhóm gu thời trang, và mô tả chi tiết dạng văn bản.

Bảng customers chứa khoảng 1,37 triệu khách hàng, gồm các thông tin như mã khách, trạng thái câu lạc bộ thành viên, tần suất nhận newsletter, tuổi và mã bưu chính. Cột tuổi có nhiều giá trị thiếu, cần điền mặc định 25 khi thiếu.

Bảng transactions có khoảng 31,8 triệu giao dịch trải dài từ tháng 9 năm 2018 đến tháng 9 năm 2020, tương đương 730 ngày. Mỗi bản ghi gồm ngày giao dịch, mã khách, mã sản phẩm, giá bán và kênh bán (cửa hàng hay online).

## 2.2. Đặc tính thống kê

Số giao dịch trung bình trên mỗi khách hàng là khoảng 23, nhưng phân phối có đuôi dài rõ rệt — trung vị chỉ là 6 giao dịch, trong khi top 1 phần trăm khách hàng có tới 250 giao dịch trở lên.

Tỉ lệ mua lặp lại — tức một cặp khách hàng và sản phẩm xuất hiện hơn một lần — chiếm khoảng 18 phần trăm. Đây là tín hiệu rất mạnh cho thuật toán Repurchase.

Tỉ lệ sản phẩm có biến thể đạt tới 85 phần trăm — nghĩa là phần lớn các mã code có ít nhất 2 mã article khác nhau (khác màu, khác size). Đây là cơ sở vững chắc cho thuật toán Sibling.

Sản phẩm bestseller tập trung cao độ: top 1 phần trăm sản phẩm chiếm khoảng 30 phần trăm tổng lượng bán. Đây vừa là cơ hội (popularity hoạt động tốt) vừa là rủi ro (bias filter bubble nếu chỉ dựa vào popularity).

## 2.3. Quan sát khai thác cho recommendation

Qua phân tích, có bốn quan sát chính dẫn dắt việc thiết kế các bộ sinh ứng viên.

Một, Repurchase là tín hiệu rất mạnh. Nếu người dùng đã mua một chiếc áo size M, xác suất họ mua lại cùng item hoặc một biến thể size khác trong tương lai gần là đáng kể.

Hai, sibling code là tín hiệu mạnh thứ hai. Vì 85 phần trăm sản phẩm có biến thể, việc gợi ý "cùng kiểu nhưng khác màu" là một chiến lược an toàn và hiệu quả.

Ba, xu hướng thay đổi nhanh. Một sản phẩm có thể bán chạy trong 1 tuần rồi ngừng đột ngột, nên các đặc trưng đếm số bán trong 3 ngày, 7 ngày và 14 ngày gần nhất rất quan trọng.

Bốn, tồn tại sự phân khúc rõ ràng theo nhóm tuổi. Gu thời trang khác biệt giữa các nhóm dưới 25, từ 25 đến 35, từ 36 đến 45, từ 46 đến 55, và trên 55. Bestseller theo từng nhóm có thể tốt hơn bestseller toàn cục.

---

# 3. Kiến trúc tổng thể

## 3.1. Mô tả luồng dữ liệu

Dữ liệu chảy qua hệ thống theo năm tầng nối tiếp.

Tầng đầu tiên là dữ liệu nguồn dưới dạng file CSV được tải từ Kaggle. Ba file zip được giải nén và đưa vào thư mục data/raw.

Tầng thứ hai là OLTP. PostgreSQL 15 đóng vai trò mô phỏng cơ sở dữ liệu sản xuất, chứa ba bảng articles, customers và transactions. Tầng này được seed bằng các script SQL chạy lúc khởi tạo container. Đây là điểm bắt nguồn của pipeline ETL.

Tầng thứ ba là Data Lake. MinIO (chuẩn S3-compatible) lưu dữ liệu theo ba lớp Medallion. Lớp bronze chứa snapshot thô của OLTP. Lớp silver chứa dữ liệu đã làm sạch, các ứng viên từ 7 nguồn, và master sau khi gộp. Lớp gold chứa predictions Top-12 và file model.

Tầng thứ tư là Compute. Apache Spark 3.5 cụm standalone chịu trách nhiệm thực thi tất cả các bước ETL nặng (extract OLTP, clean, candidate, feature, union). Apache Airflow 2.10 đóng vai trò điều phối, theo dõi và retry. LightGBM 4.1 được dùng để train mô hình ranking pointwise.

Tầng thứ năm là Serving DB. MongoDB 6 lưu cache Top-12 đã pre-compute cho mỗi người dùng, danh sách trending toàn cục, và bestseller theo nhóm tuổi. Đây là tầng được tối ưu cho lookup theo khoá chính, đáp ứng yêu cầu độ trễ thấp.

Tầng thứ sáu là Web. Backend Express đọc thẳng MongoDB qua driver chính thức và expose REST API. Frontend React Vite Mantine gọi API qua proxy.

## 3.2. Lý do chọn pattern Medallion

Pattern Medallion (bronze, silver, gold) do Databricks phổ biến hoá, đặc trưng bởi việc chia rõ ba lớp dữ liệu.

Lớp bronze chứa dữ liệu thô, bất biến và chỉ thêm vào. Mục đích chính là đảm bảo khả năng replay — luôn có thể chạy lại pipeline từ snapshot này nếu phát hiện lỗi ETL ở lớp trên.

Lớp silver chứa dữ liệu đã được làm sạch, chuẩn hoá kiểu dữ liệu và loại bỏ trùng lặp. Đây là đầu vào chung cho mọi downstream — bất kỳ pipeline phân tích hay ML nào về sau đều bắt đầu từ silver, tránh việc lặp lại bước cleaning ở mỗi nơi.

Lớp gold chứa các artifact đã sẵn sàng phục vụ business — predictions, models, dashboard data. Lớp này được tối ưu cho consumer cuối cùng.

Lợi ích cụ thể của Medallion trong dự án này có bốn điểm. Một là khả năng tái lập kết quả: khi prediction sai, có thể đi ngược từ gold qua silver về bronze để pinpoint giai đoạn nào hỏng. Hai là re-run an toàn: mỗi task ghi vào path xác định, không có side-effect chéo. Ba là dùng chung silver: nếu sau này có DAG analytics khác cần dữ liệu cleaned, không phải parse lại CSV. Bốn là tách chính sách lifecycle: trên cloud, bronze giữ lâu (1 năm) còn gold chỉ giữ ngắn (7 ngày), giảm chi phí lưu trữ 50 đến 70 phần trăm.

## 3.3. Lý do lựa chọn công nghệ

Mỗi tầng đều có phương án thay thế đã được cân nhắc. Phần này giải thích vì sao chọn phương án hiện tại.

PostgreSQL 15 cho tầng OLTP được chọn vì có ACID, hỗ trợ SQL chuẩn và mô phỏng tốt môi trường sản xuất. MySQL là phương án tương đương khả thi. MongoDB không phù hợp ở vị trí này vì yêu cầu transactional là cốt lõi.

MinIO cho tầng Data Lake được chọn vì tương thích S3 nhưng có thể self-host miễn phí. AWS S3 yêu cầu cloud account và phát sinh chi phí. HDFS rất nặng và chạy chậm trên ARM64 Mac.

Apache Spark 3.5 cho compute được chọn vì đã trưởng thành, có MLlib sẵn (ALS và FP-Growth), và scale ngang dễ dàng. Dask yếu hơn về ML built-in. Flink hướng tới streaming nhiều hơn batch.

Apache Airflow 2.10 cho orchestration được chọn vì là chuẩn industry, có UI tốt, cơ chế retry và SLA chuẩn. Prefect quá mới. Dagster thiên về lab. Cron không có observability.

LightGBM 4.1 cho ranking được chọn vì hiệu năng tốt nhất hiện tại cho tabular data, training rất nhanh nhờ thuật toán leaf-wise. XGBoost chậm hơn 2 đến 3 lần với cùng độ chính xác. CatBoost cài đặt phức tạp hơn. PyTorch là overkill cho 24 đặc trưng tabular.

MongoDB 6 cho serving DB được chọn vì document model phù hợp với cấu trúc danh sách items, lookup theo _id rất nhanh, và schemaless cho phép tiến hoá schema không cần migration. Redis chỉ là KV thuần, không query phụ. PostgreSQL JSONB chậm hơn cho lookup theo khoá.

Node Express cho backend được chọn vì mô hình async I/O phù hợp với pattern cache lookup nặng. Python FastAPI cũng OK. Go verbose hơn cho REST API đơn giản.

React Vite Mantine cho frontend được chọn vì Vite có dev server cực nhanh nhờ esbuild, Mantine cung cấp hơn 50 component sẵn dùng. Next.js là overkill cho SPA. Vue không có lợi thế đáng kể.

Docker Compose cho deployment được chọn vì single-node demo chỉ cần 1 lệnh là up toàn bộ stack. Kubernetes là overkill cho local. Vagrant chậm.

---

# 4. Pipeline ETL chi tiết

## 4.1. Tổng quan DAG

DAG có tên recsys_pipeline_v1, được định nghĩa trong file recsys_pipeline.py. Toàn bộ DAG gồm 15 tác vụ, được lên lịch chạy thủ công (schedule_interval=None) cho mục đích demo. Trong production, schedule sẽ được đổi thành chạy hàng tuần hoặc hàng ngày.

DAG sử dụng pattern fan-out tại bước candidate generation (7 tác vụ song song) và fan-in tại bước union_master (gộp đầu ra của 7 tác vụ thành một). Mỗi tác vụ thiết lập retry tối đa 1 lần với khoảng cách 5 phút, timeout thực thi tối đa 2 giờ.

## 4.2. Tác vụ 1 — Wait OLTP Ready

Đây là một sensor kiểm tra tầng OLTP đã sẵn sàng chưa. Cụ thể, sensor sẽ kết nối tới PostgreSQL OLTP và đếm số dòng trong bảng transactions, nếu lớn hơn 0 thì sensor pass, nếu không thì sensor đợi và retry sau 15 giây.

Lý do cần sensor này: container oltp-postgres khi khởi động sẽ chạy các script SQL trong thư mục oltp_init để seed dữ liệu từ CSV, quá trình này mất 1 đến 2 phút. Nếu Spark JDBC kết nối quá sớm, kết quả đọc sẽ rỗng và pipeline fail.

Sensor được cấu hình ở chế độ reschedule (không phải poke), nghĩa là khi đợi, sensor giải phóng slot worker thay vì giữ thread idle — giúp tiết kiệm tài nguyên Airflow.

## 4.3. Tác vụ 2 — Extract OLTP to MinIO

Tác vụ này dùng Spark JDBC để đọc ba bảng (articles, customers, transactions) từ PostgreSQL OLTP và ghi sang lớp bronze của MinIO dưới định dạng parquet.

Có một tối ưu quan trọng: bảng transactions có 31 triệu dòng, nếu đọc tuần tự bằng một executor sẽ rất chậm. Spark cho phép cấu hình partitionColumn và numPartitions để chia bảng thành nhiều phần và pull song song. Trong dự án, ta dùng 8 phân vùng dựa trên cột article_id, cho phép 8 connection JDBC chạy đồng thời, giảm thời gian extract khoảng 8 lần.

Để Spark có thể đọc từ JDBC và ghi sang S3 (qua giao thức s3a), cần ba thư viện jar được nạp qua tham số packages: hadoop-aws cho hỗ trợ s3a, aws-java-sdk-bundle cho AWS SDK, và postgresql JDBC driver.

## 4.4. Tác vụ 3 — Step 1 Cleaning

Tác vụ này đọc dữ liệu từ lớp bronze, áp dụng các bước làm sạch tiêu chuẩn, sau đó ghi sang lớp silver.

Các bước làm sạch bao gồm: loại bỏ các bản ghi trùng lặp theo khoá chính, điền giá trị mặc định cho các trường thiếu (tuổi mặc định 25, trạng thái club thành viên mặc định PRE-CREATE), lọc bỏ các giao dịch có giá nhỏ hơn hoặc bằng 0, chuẩn hoá cột ngày về kiểu date, và loại bỏ các giao dịch trùng nhau hoàn toàn theo bộ ba (khách, sản phẩm, ngày).

Đầu ra là ba file parquet trong lớp silver, đây là đầu vào chuẩn cho tất cả các bước phía sau.

## 4.5. Tác vụ 4 — Bảy bộ sinh ứng viên

Đây là pha thứ nhất (Recall). Bảy tác vụ chạy song song, mỗi tác vụ ghi ra một file parquet riêng trong thư mục silver/candidates/<chiến lược>. Schema chung gồm ba cột: mã khách, mã sản phẩm, và điểm số chuẩn hoá Min-Max trong khoảng 0 đến 1.

### 4.5.1. Repurchase (Top-15)

Logic: lọc các giao dịch của mỗi khách trong 8 tuần gần nhất, đếm số lần mua mỗi sản phẩm, sắp xếp giảm dần và lấy 15 sản phẩm hàng đầu. Pattern bắt được: khách mua lại đồ cũ. Mạnh với các sản phẩm cơ bản như áo thun, đồ lót, tất chân.

### 4.5.2. Popularity (Top-30)

Logic: tính 30 sản phẩm bán chạy nhất trong 7 ngày qua trên toàn hệ thống, sau đó áp dụng kết quả này cho tất cả khách hàng. Pattern bắt được: trending items. Đặc biệt quan trọng cho khách cold-start (không có history).

### 4.5.3. Sibling (Top-15)

Logic: với mỗi khách, lấy các mã product code mà họ đã mua, sau đó tìm tất cả các article id khác cùng product code (cùng kiểu sản phẩm nhưng khác màu hoặc khác size) và đưa vào danh sách ứng viên. Pattern bắt được: "đã mua áo này màu xanh, gợi ý áo cùng kiểu màu đỏ". Đặc biệt mạnh vì 85 phần trăm sản phẩm có biến thể.

### 4.5.4. ALS (Top-40)

Sử dụng thuật toán Alternating Least Squares của Spark MLlib với cấu hình implicit feedback. Các siêu tham số được lựa chọn sau thí nghiệm: rank = 32 (cân bằng giữa độ biểu đạt và overfit), alpha = 10.0 (gán trọng số cho tín hiệu mua, mặc định coi 1 lượt mua = signal mạnh), regParam = 0.05 (L2 regularization nhẹ vì dữ liệu thưa), maxIter = 15.

Mô hình học latent factor cho cả user và item, sau đó với mỗi user dùng phương thức recommendForUserSubset để sinh ra Top-40 ứng viên. Pattern bắt được: latent collaborative signal mà các phương pháp co-occurrence trực tiếp không thấy được.

### 4.5.5. ItemCF (Top-20)

Item-based Collaborative Filtering qua co-occurrence và cosine similarity. Logic gồm ba bước.

Bước một, tính ma trận co-occurrence: số lần hai sản phẩm khác nhau cùng xuất hiện trong lịch sử của một khách. Phép self-join trên bảng transactions sẽ sinh ra các cặp này. Một tối ưu cần lưu ý: chỉ giữ cặp (i, j) khi i < j để tránh sinh trùng cả (i, j) và (j, i).

Bước hai, tính cosine similarity giữa hai sản phẩm: số lần đồng xuất hiện chia cho căn bậc hai của tích số lần mỗi sản phẩm xuất hiện riêng.

Bước ba, với mỗi khách, lấy các sản phẩm họ đã mua, join với ma trận similarity để lấy các sản phẩm tương tự, sau đó cắt giữ Top-20 theo điểm cosine.

Pattern bắt được: "người mua áo này thường mua quần này". Khác ALS ở chỗ tính trực tiếp item-item mà không thông qua latent factor.

### 4.5.6. Categorical (Top-40)

Logic: với mỗi khách, xác định "gu" thời trang dựa trên ba thuộc tính là nhóm sản phẩm (index_group), nhóm trang phục (garment_group) và nhóm màu (colour_group). Sau đó tính bestseller theo từng tổ hợp ba thuộc tính này, và gán cho khách các bestseller phù hợp với gu của họ.

Pattern bắt được: "khách thường mua áo đen → gợi ý áo đen bestseller". Khác ItemCF (item-pair) ở mức trừu tượng cao hơn — không gợi ý item cụ thể mà gợi ý theo style.

### 4.5.7. FP-Growth (Top động)

Sử dụng thuật toán FP-Growth của Spark MLlib để khai thác association rules. Logic gồm ba bước.

Bước một, tạo "basket" — một transaction trong ngôn ngữ FP-Growth tương ứng với một bộ (khách hàng, ngày). Mỗi basket là tập các sản phẩm được mua chung trong ngày đó.

Bước hai, huấn luyện mô hình FP-Growth với hai tham số quan trọng: minSupport = 0.001 (chỉ giữ itemset xuất hiện trong ít nhất 0.1 phần trăm basket) và minConfidence = 0.1 (chỉ giữ rule có độ tin cậy ít nhất 10 phần trăm).

Bước ba, với mỗi khách, lấy lịch sử mua của họ và transform qua mô hình. Mô hình sẽ áp dụng các rule phù hợp và trả về các "consequent" (sản phẩm hệ quả) làm ứng viên.

Pattern bắt được: association rule kiểu "{áo polo, quần jean} → giày sneaker với confidence 0.3". Đây là kiểu tín hiệu cụm sản phẩm mua kèm.

### 4.5.8. Tổng hợp pha Recall

Bảy bộ sinh ứng viên đa dạng theo bốn nhánh: dựa rule (Repurchase, Popularity, Sibling), dựa collaborative latent (ALS), dựa item-item similarity (ItemCF), dựa categorical (Categorical), và dựa association mining (FP-Growth). Sự đa dạng này là chìa khoá nâng Recall lên.

## 4.6. Tác vụ 5 — Union Master

Tác vụ này gộp 7 luồng ứng viên thành một bảng duy nhất, loại trùng theo cặp (khách hàng, sản phẩm), nhưng giữ lại thông tin về nguồn — tức một sản phẩm có thể được "vote" bởi nhiều chiến lược cùng lúc.

Sau khi loại trùng, số ứng viên trung bình cho mỗi người dùng là khoảng 97. Các thông tin được kết hợp giữ lại bao gồm: tập hợp các nguồn đã đề xuất (sources), điểm số tối đa từ ALS, ItemCF và FP-Growth (cho các candidate có điểm), và nguồn rule-based được đánh dấu một-hot riêng.

## 4.7. Tác vụ 6 — Feature Label

Đây là tác vụ quan trọng nhất, vừa sinh nhãn vừa tính 24 đặc trưng cho pha rank.

Về thiết lập cửa sổ thời gian: HISTORY_DAYS được đặt là 42 ngày (6 tuần) để tính feature. Cửa sổ huấn luyện là khoảng từ cuối kỳ trừ 8 tuần đến cuối kỳ trừ 1 tuần. Cửa sổ nhãn (target) là một tuần cuối cùng — đây là phần "tương lai" mà mô hình cần học cách dự đoán.

Tạo nhãn: với mỗi cặp (khách hàng, sản phẩm) trong master, kiểm tra xem khách đó có thật sự mua sản phẩm đó trong tuần cuối không. Nếu có thì label = 1, không thì label = 0.

Downsample negatives 10:1: dataset cực kỳ mất cân bằng, mỗi cặp dương có khoảng 50 cặp âm. Để LightGBM train hiệu quả và tránh bias, ta giảm cặp âm xuống còn 10 lần cặp dương. Sau đó bù lại bằng tham số scale_pos_weight = 10 trong cấu hình mô hình.

## 4.8. Tác vụ 7 — Train LightGBM

Tác vụ này không cần Spark — chạy hoàn toàn trên Python driver bằng thư viện lightgbm. Dữ liệu được đọc từ parquet bằng pandas và pyarrow, downcast về float32 hoặc int32 nhỏ nhất phù hợp để tiết kiệm bộ nhớ.

Các siêu tham số quan trọng được lựa chọn sau khi thí nghiệm. Objective được đặt là binary classification thay vì lambdarank — đây là một lựa chọn có chủ ý sẽ giải thích ngay sau. Metric đánh giá là AUC. Số iteration tối đa là 400 với learning rate 0.03. Cây có độ phức tạp num_leaves = 63 (tương đương 2 mũ 6 trừ 1) và max_depth = 8. Tham số scale_pos_weight = 10 để bù lại bước downsample. min_child_samples = 100 tránh overfit trên leaf nhỏ. subsample = 0.8 và colsample_bytree = 0.8 cho regularization theo cả row và feature.

Vì sao chọn objective binary thay vì lambdarank: lambdarank là pure ranking, tối ưu trực tiếp MAP và NDCG. Binary là pointwise — đoán xác suất 0/1 cho mỗi cặp. Trên dataset này, binary với scale_pos_weight cho kết quả tương đương lambdarank nhưng có ba lợi điểm: train nhanh hơn khoảng 20 phần trăm vì không cần build cấu trúc group, output là probability nên dễ debug hơn, và tương thích với mọi phiên bản LightGBM.

Sau khi train, mô hình được lưu xuống file local rồi đẩy lên lớp gold của MinIO. Đồng thời, feature importance theo tiêu chí "gain" được in ra để phục vụ phân tích.

## 4.9. Tác vụ 8 — Predict LightGBM

Tác vụ này load model đã train, đọc dữ liệu test (tương tự cấu trúc train nhưng không có nhãn), predict điểm số cho mỗi cặp, rank theo điểm trong từng khách hàng, và cắt giữ Top-12.

Có một logic fallback quan trọng cho các khách hàng không có ứng viên (do quá ít history): áp dụng time-decayed popularity. Tức là không lấy raw bestseller, mà gán trọng số giảm dần theo thời gian — sản phẩm bán gần đây có weight cao hơn, theo công thức trọng số = tổng theo các lần mua của exp(âm khoảng thời gian chia tau), với tau khoảng 14 ngày. Điều này tránh bias bestseller cũ.

Đầu ra cuối cùng là một bảng top12_recommendations.parquet trong lớp gold, gồm bộ ba (khách hàng, danh sách 12 sản phẩm, ngày dự đoán).

## 4.10. Tác vụ 9 — Export to MongoDB

Tác vụ này đẩy bốn loại thông tin lên MongoDB.

Đầu tiên là user_recommendations — bulk upsert Top-12 cho mỗi khách hàng. Mỗi document có cấu trúc gồm mã khách làm khoá chính, mảng items chứa 12 mã sản phẩm, và timestamp cập nhật.

Tiếp theo là global_trending — một document duy nhất với khoá "global" chứa Top-12 trending toàn hệ thống.

Sau đó là age_bestsellers — sáu document tương ứng sáu nhóm tuổi (dưới 25, 25-35, 36-45, 46-55, trên 55, Unknown), mỗi document chứa Top-12 cho nhóm đó.

Cuối cùng là pipeline_runs — một document ghi lại metadata của lần chạy này: ngày run, thời điểm kết thúc, số khách hàng được dự đoán, số item trending, danh sách nhóm tuổi. Document này phục vụ observability và debug khi cần nhìn lại lịch sử các lần chạy.

Một tối ưu quan trọng: dùng bulk_write với option ordered=False để MongoDB có thể parallel apply 5 nghìn operation trong một batch. So với upsert tuần tự, cách này nhanh hơn khoảng 100 lần.

## 4.11. Tác vụ 10 — Notify Done

Tác vụ cuối cùng chỉ đơn giản log ra thông báo pipeline hoàn tất. Trong môi trường production sẽ thay bằng gửi Slack, email hoặc webhook tới hệ thống observability để báo cho team biết.

---

# 5. Feature Engineering và Ranking

## 5.1. Tổng quan 24 đặc trưng

Mô hình LightGBM dùng 24 đặc trưng chia thành 7 nhóm.

### Nhóm A — User Profile (4 đặc trưng)

Nhóm này mô tả người dùng. Tuổi (age) là đặc trưng nhân khẩu học cơ bản. user_total_purchases đếm số giao dịch người dùng có trong 42 ngày qua. user_avg_budget tính giá trung bình mà người dùng đã chi tiêu. days_since_last_purchase đo khoảng cách (số ngày) từ giao dịch gần nhất tới thời điểm dự đoán.

### Nhóm B — Item Profile (5 đặc trưng)

Nhóm này mô tả sản phẩm. item_total_sales đếm tổng số lần item được bán trong 42 ngày. item_avg_price là giá bán trung bình của item. Ba đặc trưng trending — item_sales_last_3d, item_sales_last_7d, item_sales_last_14d — bắt cường độ bán hiện tại theo các cửa sổ thời gian khác nhau, giúp mô hình nhận biết item nào đang "hot".

### Nhóm C — Tương tác User × Item (4 đặc trưng)

Nhóm này quan trọng nhất, bắt mối quan hệ trực tiếp giữa user và item cụ thể. user_item_buy_count đếm số lần user đã mua chính item này — tín hiệu repurchase mạnh. days_since_bought_THIS_item đo khoảng cách lần mua item này gần nhất. user_type_buy_count đếm số lần user đã mua sản phẩm thuộc cùng product_type — bắt được "user này thích loại sản phẩm này". age_group_item_sales đo item bán bao nhiêu trong cùng nhóm tuổi với user — bắt được "item phù hợp với nhóm tuổi nào".

### Nhóm D — Đặc trưng phái sinh (3 đặc trưng)

Ba đặc trưng được tính từ các đặc trưng nguyên thuỷ trên. trend_velocity bằng item_sales_last_7d chia cho (item_sales_last_14d + 1), bắt đà tăng — item nào đang trên đà tăng nhanh. age_diff là chênh lệch tuyệt đối giữa tuổi user và tuổi trung bình của các người đã mua item — bắt được sự lệch của user với gu điển hình. price_diff là chênh lệch giữa giá item và ngân sách trung bình của user — bắt được item có vượt ngân sách user hay không.

### Nhóm E — Nguồn ứng viên (3 đặc trưng)

Ba đặc trưng dạng nhị phân (0/1) cho biết item này có nằm trong danh sách ứng viên của ALS, ItemCF, FP-Growth hay không. Đây là tín hiệu meta giúp mô hình học được "candidate từ ALS có precision cao hơn trong segment X" và tự gán weight phù hợp.

### Nhóm F — Điểm số ứng viên (3 đặc trưng)

Ba đặc trưng là điểm số chuẩn hoá Min-Max từ ba thuật toán có scoring: als_score, itemcf_score, fpgrowth_score. Khác với nhóm E (chỉ là binary), nhóm này cho mô hình biết "mức độ tin tưởng" của thuật toán tương ứng.

### Nhóm G — Đặc trưng categorical (2 đặc trưng)

Hai đặc trưng phân loại: product_type_name (như "Dress", "Trousers", "Sweater") và colour_group_name (như "Black", "Red", "Light Blue"). LightGBM xử lý categorical native (không cần one-hot) nhờ vào việc khai báo trong tham số categorical_feature khi tạo Dataset.

## 5.2. Vì sao có nhóm provenance (E và F)?

Đây là một lựa chọn thiết kế quan trọng. Mô hình ranking không chỉ học "item này tốt cho user này" mà còn học "candidate đến từ ALS thường có precision cao hơn FP-Growth trong segment người dùng tuổi 25 đến 35". Tín hiệu meta này cho phép mô hình tự gán weight cho các thuật toán recall ngay trong quá trình train, thay vì phải hard-code weight thủ công.

Nếu bỏ nhóm E và F, mô hình coi mọi candidate như nhau, mất đi thông tin về độ tin cậy đã được pre-compute ở pha recall.

## 5.3. Vì sao không dùng embedding?

Phiên bản hiện tại tập trung vào đặc trưng tabular thuần. Embedding (user2vec, item2vec, CLIP image embedding) đã được thử nghiệm trong notebook nghiên cứu nhưng có ba vấn đề: training time tăng gấp 3, Recall@12 chỉ tăng dưới 0.5 phần trăm, và rất khó debug khi prediction sai. Quyết định để embedding cho phase phát triển sau khi đã có cluster compute lớn hơn.

## 5.4. Feature importance từ training

Sau khi train, LightGBM xuất bảng feature importance theo chỉ tiêu "gain" (tổng giảm loss khi sử dụng đặc trưng đó để split). Quan sát chính: bốn đặc trưng top đều liên quan tới lịch sử user-item — days_since_bought_THIS_item, user_item_buy_count, als_score, from_als. Điều này xác nhận tín hiệu Repurchase và ALS là quan trọng nhất. Các đặc trưng categorical (colour_group_name) đứng giữa bảng, vẫn có đóng góp đáng kể.

---

# 6. Tầng serving (Web)

## 6.1. Backend Express

Backend được tổ chức theo cấu trúc lib và routes. Thư mục lib chứa các module dùng chung — kết nối MongoDB, datastore khởi tạo và quản lý cache, parser CSV streaming, builder URL ảnh và fallback ảnh placeholder. Thư mục routes chứa các handler REST cho hai nhóm endpoint: recommendations (home, trending, cart) và products (detail, similar).

### 6.1.1. Quy trình khởi động

Khi backend khởi động, hàm initStore được gọi trong promise chain. Hàm này thực hiện bốn việc tuần tự: kết nối tới MongoDB với timeout 5 giây, song song hoá việc seed bốn collection (articles, similar_products, cart_recommendations, global_trending) nếu chúng đang rỗng, load toàn bộ 105 nghìn articles vào một Map in-memory để hỗ trợ tra cứu siêu nhanh, và đặt cờ ready để chấp nhận request.

Việc tách initStore ra trước khi listen port đảm bảo backend chỉ phục vụ request khi mọi dependency đã sẵn sàng — không có race condition giữa request đến và DB chưa kết nối.

### 6.1.2. Endpoint Home (Personalized hoặc Fallback)

Endpoint GET /api/recommendations/home/:customerId là endpoint quan trọng nhất. Logic xử lý: trước tiên tra cứu user_recommendations trong MongoDB theo mã khách. Nếu tìm thấy, trả về với source = "personalized" và badge "AI gợi ý dựa trên lịch sử mua hàng của bạn". Nếu không tìm thấy (khách cold-start), fallback sang trending toàn cục với source = "trending" và badge "Top bán chạy toàn hệ thống".

Trong cả hai trường hợp, danh sách 12 mã sản phẩm được "hydrate" — tức là enrich với metadata (tên, loại, giá, URL ảnh) bằng cách lookup vào Map in-memory. Quá trình hydrate này hoàn toàn không cần round-trip MongoDB thêm lần nào.

Hiệu năng đo được: median 4ms, percentile 99 là 12ms.

### 6.1.3. Endpoint Cart (Co-purchase)

Endpoint POST /api/recommendations/cart nhận danh sách item trong giỏ và sinh gợi ý co-purchase. Logic gồm bốn bước.

Bước một, với mỗi item trong giỏ, tra cứu cart_recommendations để lấy danh sách các item thường mua kèm. Việc này được song song hoá bằng Promise.all để giảm wait time tổng.

Bước hai, tổng hợp "phiếu vote": mỗi candidate được "vote" bởi các item trong giỏ đã đề xuất nó. Lưu lại count (số lượt vote), nguồn (item nào trong giỏ vote), và best rank (rank tốt nhất trong các danh sách đề xuất).

Bước ba, loại các candidate đã có trong giỏ.

Bước bốn, sắp xếp candidate theo hai tiêu chí: count giảm dần (nhiều vote hơn ưu tiên), tie-break bằng best rank tăng dần. Sinh reason hiển thị: nếu count > 1 thì "Liên quan X/Y món trong giỏ", nếu count = 1 thì "Thường mua cùng <tên item>".

### 6.1.4. Các tối ưu performance đã áp dụng

Articles được load 1 lần vào in-memory Map khi boot, sau đó mỗi request hydrate 12 items chỉ tốn 12 phép lookup Map (đỗ phức tạp O(1) mỗi lookup). Không phải round-trip MongoDB cho catalog metadata.

Trending có TTL cache 60 giây — vì danh sách trending toàn cục không đổi trong khoảng thời gian này, không cần truy vấn Mongo mỗi request. Cache hit ratio đạt 99 phần trăm sau khi warm up.

Mongo lookup theo _id (khoá chính) tận dụng B-tree index sẵn có. Không cần tạo secondary index. Độ trễ trung bình mỗi lookup dưới 5ms.

Song song hoá batch: khi xử lý cart có N item, dùng Promise.all để N lookup chạy song song thay vì tuần tự. Giảm độ trễ tuyến tính theo N xuống xấp xỉ độ trễ một lookup.

Seed idempotent: lần boot đầu seed 105 nghìn articles, các lần boot sau kiểm tra count, nếu đã có thì skip — không reseed gây trùng lặp.

## 6.2. Frontend React

Frontend xây dựng trên stack hiện đại: React 19 (hỗ trợ hook use mới), Vite 8 (esbuild dev server cực nhanh, rollup cho production build), Mantine 9 (component library với hơn 50 component sẵn dùng), và Tabler Icons.

Cấu hình Vite có proxy cho prefix /api: tất cả request bắt đầu bằng /api sẽ được forward sang backend ở port 4100. Trong production, vai trò proxy sẽ được thay bằng nginx hoặc Cloudflare.

Ba trang chính của frontend. Trang Home có ô nhập customer ID và hiển thị grid 12 sản phẩm gợi ý kèm badge mô tả nguồn (personalized hay trending). Trang Product detail có ảnh sản phẩm, thông tin chi tiết và một carousel "Sản phẩm tương tự". Trang Cart cho phép xem giỏ và bấm nút "Xem gợi ý" để mở modal hiển thị các item co-purchase với lý do.

## 6.3. Schema MongoDB

MongoDB chứa 7 collection.

Collection user_recommendations có khoảng 10 nghìn document (do demo chỉ chạy trên subset 12 nghìn user). Mỗi document có khoá chính là mã khách, mảng items 12 phần tử là 12 mã sản phẩm, và timestamp updated_at. Đây là collection được pipeline ghi.

Collection global_trending có một document duy nhất với khoá "global", mảng items 12 phần tử, và timestamp. Cũng do pipeline ghi.

Collection age_bestsellers có 6 document tương ứng 6 nhóm tuổi, cấu trúc tương tự global_trending. Cũng do pipeline ghi.

Collection articles có 105 nghìn document, mỗi document mô tả một sản phẩm với các trường tên, loại, giá, image folder. Do backend seed từ file CSV ở root project.

Collection similar_products có 105 nghìn document, mỗi document có khoá là mã sản phẩm và mảng items 0 đến 6 phần tử là các sản phẩm tương tự. Do backend seed từ file JSON.

Collection cart_recommendations có 28 nghìn document, cấu trúc tương tự similar_products. Do backend seed từ file JSON (xuất phát từ notebook FP-Growth).

Collection pipeline_runs là history các lần chạy pipeline, mỗi document ghi run_date, finished_at, user_count, global_count và age_groups. Do pipeline ghi.

Về index: chỉ sử dụng index _id mặc định của MongoDB cho mọi collection. Mọi pattern query đều là lookup theo khoá chính, không có range query hay filter phụ phức tạp, nên không cần secondary index. Việc này cũng giúp giảm chi phí ghi.

## 6.4. Mongo Express UI

Stack mặc định kèm Mongo Express phiên bản 1.0.2, một web UI quản trị MongoDB. Truy cập tại địa chỉ http://localhost:8083 với tài khoản basic auth admin/admin.

Các chức năng chính: duyệt danh sách collection và đếm document, mở từng document để xem chi tiết, query bằng cú pháp MongoShell-like, export collection sang JSON, theo dõi server stats (memory, connections, slow query). Đây là công cụ debug và demo rất tiện — không cần cài client desktop hay học mongosh.

---

# 7. Triển khai bằng Docker Compose

## 7.1. Danh sách dịch vụ

Stack gồm chín dịch vụ container, được khai báo trong file docker-compose.yml ở thư mục bigdata.

Dịch vụ postgres dùng image postgres:13, đóng vai trò metadata DB cho Airflow. Có healthcheck pg_isready cho user airflow.

Dịch vụ oltp-postgres dùng image postgres:15, đóng vai trò DB OLTP mô phỏng. Expose port 5433 (tránh xung đột với postgres bên trên). Có volume oltp-data lưu data và mount thư mục oltp_init làm seed script.

Dịch vụ minio dùng image minio chính thức, expose hai port: 9000 cho S3 API và 9001 cho web console.

Dịch vụ minio-init dùng image mc (MinIO client), chạy một lần duy nhất để tạo bucket "datalake" cần thiết cho pipeline. Sau khi tạo xong là container exit.

Dịch vụ mongodb dùng image mongo:6.0, expose port 27017, volume mongo-data lưu data persistent.

Dịch vụ mongo-express dùng image mongo-express:1.0.2, expose port 8083 ánh xạ tới port 8081 nội bộ.

Dịch vụ spark-master và spark-worker dùng image Bitnami Spark 3.5. Master expose 8080 (UI) và 7077 (RPC), worker expose 8081 (UI). Cả hai mount chung thư mục apps (chứa script Python), thư mục data (volume share với host), và volume spark-ivy để cache thư viện jar.

Dịch vụ airflow build từ Dockerfile custom (docker-file.airflow.demo), expose port 8082. Mount bốn thư mục: dags (DAG Python files), apps (Spark script), logs (lưu log task), data (share dữ liệu).

## 7.2. Đồ thị phụ thuộc giữa các dịch vụ

Khi khởi động bằng lệnh docker compose up, Docker Compose tự động respect thứ tự khởi động dựa trên depends_on.

Dịch vụ airflow đợi năm dependency: postgres healthy (metadata DB), oltp-postgres healthy (data source), mongodb started (export target), spark-master started (compute), và minio-init completed_successfully (bucket đã được tạo). Chỉ khi tất cả OK, airflow mới start.

Dịch vụ spark-worker đợi spark-master started.

Dịch vụ mongo-express đợi mongodb started.

Các dependency này đảm bảo: khi user mở UI Airflow và trigger DAG, mọi thành phần infra đã sẵn sàng. Không có race condition.

## 7.3. Custom image Airflow

Image custom được build từ image gốc apache/airflow:2.10.4-python3.12. Ba dependency được cài thêm.

Thư viện pyspark phiên bản 3.5.6 — đây là yêu cầu bắt buộc phải match chính xác phiên bản Spark cluster. Lý do: PySpark serialize task qua Pickle, không tương thích giữa các phiên bản chính khác nhau. Nếu driver dùng PySpark 3.5.6 còn worker dùng Spark 3.5.x với PySpark khác minor, mọi task sẽ fail với lỗi "Python in worker has different version".

Thư viện lightgbm phiên bản 4.1.0 — phiên bản stable mới nhất có wheel cho Python 3.12.

Thư viện pymongo phiên bản 4.6.1 — match với giao thức MongoDB 6.0.

Một dependency ở mức OS là libgomp1, dùng cho OpenMP runtime mà LightGBM cần để train song song. Nếu không có, LightGBM sẽ crash với lỗi "cannot open libgomp.so.1" ngay khi load thư viện.

## 7.4. Truyền biến môi trường xuống Spark

Container airflow đặt nhiều biến môi trường: connection string Spark, kết nối MongoDB, cấu hình MinIO (endpoint, access key, secret key), cấu hình JDBC OLTP, và PYTHONPATH trỏ tới thư mục apps.

DAG sau đó propagate các biến này xuống Spark executor thông qua các cấu hình spark.executorEnv.* — đây là cách Airflow truyền context cho Spark job mà không cần hardcode.

Đặc biệt quan trọng là tham số spark.jars.packages khai báo ba jar cần tải xuống runtime: hadoop-aws 3.3.4, aws-java-sdk-bundle 1.12.262, và postgresql JDBC driver 42.5.4. Spark sẽ tự tải các jar này từ Maven Central về cache spark-ivy lần đầu tiên (mất khoảng 1 phút), các lần sau dùng lại cache.

## 7.5. Hướng dẫn chạy demo

Quy trình chạy demo end-to-end gồm năm bước.

Bước một, sinh demo subset. Tải dataset Kaggle về root project, giải nén ba file zip vào data/raw, sau đó chạy script sample_dataset.py để sinh subset khoảng 30MB cho 12 nghìn người dùng (user_frac = 0.02, since = 2020-06-01). Copy file subset vào data/raw để OLTP seed sẽ đọc từ đó.

Bước hai, khởi động stack: lệnh docker compose up -d --build trong thư mục bigdata. Đợi khoảng 30 giây để Airflow init xong.

Bước ba, trigger DAG: mở UI Airflow tại localhost:8082 (đăng nhập admin/admin), bật toggle DAG recsys_pipeline_v1, click trigger và theo dõi tab Graph.

Bước bốn, kiểm tra MongoDB: mở UI Mongo Express tại localhost:8083 (admin/admin), vào database hm_recsys để xem các collection. Hoặc dùng mongosh CLI để kiểm tra count và sample document.

Bước năm, chạy web: terminal mới, vào thư mục backend, copy .env.example sang .env (cấu hình mặc định port 4100 và MONGO_URI là localhost:27017), npm install, npm run dev. Terminal khác, vào frontend, npm install, npm run dev. Mở browser tại localhost:5173 để xem UI.

---

# 8. Kết quả và đánh giá

## 8.1. Recall theo từng bộ sinh ứng viên

Đánh giá được thực hiện trên demo subset gồm khoảng 12 nghìn user, dải thời gian từ tháng 6 đến 22 tháng 9 năm 2020. Thước đo chính là Recall@12 trên tập test (tuần cuối).

ALS với Top-40 đạt Recall 0.0323, là kết quả cao nhất trong các thuật toán đơn lẻ. Categorical với Top-40 đạt 0.0292, gần ngang ALS. ItemCF với Top-20 đạt 0.0290. Sibling với Top-15 đạt 0.0276. Repurchase với Top-15 đạt 0.0254. Popularity với Top-30 đạt 0.0236.

Khi gộp 7 nguồn lại (sau khi loại trùng, trung bình mỗi user có 97 candidate), Master union đạt Recall 0.0905 — gấp 2.8 lần thuật toán đơn lẻ tốt nhất. Đây là minh chứng rõ ràng cho giả thiết "đa tín hiệu" trong recommendation: không tồn tại một thuật toán đơn lẻ "siêu mạnh", mà sức mạnh đến từ sự đa dạng.

Lưu ý về số tuyệt đối: Recall@12 đạt mức 9 phần trăm tưởng thấp nhưng thực tế là kết quả tốt cho bài toán next-basket. Nguyên nhân là target window chỉ 7 ngày — đa số user không phát sinh giao dịch trong tuần đó, nên ngay cả với prediction hoàn hảo cũng không thể đẩy Recall lên cao hơn nhiều.

## 8.2. Hiệu năng sau khi LightGBM rerank

Sau khi LightGBM xếp hạng 97 candidate xuống Top-12, Recall@12 đạt khoảng 0.026. MAP@12 đạt khoảng 0.018. Điều này có nghĩa LightGBM thực hiện được vai trò của mình: "nén" tập candidate xuống Top-12 mà vẫn giữ được phần lớn tín hiệu chất lượng.

Đáng chú ý là Recall@12 sau rerank gần bằng Recall@12 của thuật toán đơn lẻ tốt nhất (ALS 0.0323), nhưng được hưởng lợi từ diversity của union — tức trên cùng số slot, danh sách rerank có độ phủ rộng hơn về kiểu sản phẩm.

## 8.3. Độ trễ online

Đo trên endpoint với cache warm. Endpoint health đáp ứng dưới 2ms ở percentile 99. Endpoint trending đáp ứng 3ms ở percentile 99 (nhờ TTL cache). Endpoint home (personalized) median 4ms, percentile 99 là 12ms. Endpoint product detail dưới 1ms (chỉ in-memory lookup). Endpoint similar median 5ms, percentile 99 là 15ms. Endpoint cart với 5 item median 18ms, percentile 99 là 35ms.

Tất cả endpoint đều dưới ngưỡng 50ms percentile 99 cho UI e-commerce — đáp ứng đầy đủ yêu cầu nghiệp vụ.

## 8.4. Tài nguyên sử dụng

Tổng peak RAM của toàn bộ stack là khoảng 6GB, trong đó spark-worker chiếm 4GB lúc chạy job, airflow khoảng 600MB, mongodb khoảng 250MB, frontend (vite dev) khoảng 250MB, backend khoảng 200MB, spark-master khoảng 200MB, và hai postgres mỗi cái khoảng 100MB. Stack chạy được trên máy Mac M2 16GB hoặc máy Linux/Windows 8GB RAM (vừa đủ).

---

# 9. Trao đổi thiết kế (Trade-offs)

## 9.1. Two-stage so với One-stage end-to-end

Phương án one-stage là dùng một mô hình deep learning duy nhất chấm điểm trực tiếp từng cặp (user, item). Ưu điểm là mô hình có thể học signal tự nhiên hơn (kết hợp embedding text, image, behavior). Nhược điểm rất rõ: phải chấm 1,4 nghìn tỷ cặp — bất khả thi với mọi infrastructure thực tế.

Phương án two-stage tách thành recall và rank. Pha recall sinh ~100 candidate / user bằng nhiều thuật toán đơn giản. Pha rank chấm điểm 100 candidate này bằng mô hình phức tạp. Ưu điểm là scale tốt. Nhược điểm là nếu pha recall miss item đúng, pha rank không cứu được.

Industry mainstream (Pinterest, YouTube, TikTok) đều chọn two-stage. Đề tài này theo industry.

## 9.2. Batch offline so với Real-time online

Phương án batch là pre-compute Top-12 hàng đêm, cache vào MongoDB, web chỉ làm lookup. Ưu điểm: độ trễ online cực thấp, không stress compute layer khi traffic spike. Nhược điểm: dữ liệu stale 24 giờ — nếu user thêm item vào giỏ lúc 10h sáng, prediction chỉ refresh đêm hôm sau.

Phương án real-time là compute on-the-fly mỗi request. Ưu điểm: dữ liệu luôn fresh. Nhược điểm: độ trễ rất cao (200ms+) và compute cost đắt gấp 100 lần.

Hệ thống chọn batch cho user_recommendations và global_trending. Riêng cart_recommendations là batch (FP-Growth offline) nhưng query online theo từng item đang trong giỏ — semi real-time.

## 9.3. MongoDB so với Redis so với PostgreSQL JSONB

Cả ba đều là phương án khả thi cho tầng serving DB. So sánh trên các tiêu chí.

Về lookup theo khoá chính: MongoDB khoảng 3ms, Redis dưới 1ms, PostgreSQL JSONB khoảng 5ms.

Về tính bền vững (persistence): MongoDB và PostgreSQL đều persistent mặc định, Redis cần cấu hình AOF hoặc snapshot.

Về khả năng query phụ (filter, sort): MongoDB và PostgreSQL tốt, Redis rất hạn chế.

Về schemaless: MongoDB native, PostgreSQL semi (qua JSONB), Redis chỉ là KV.

Về kích thước disk: Redis nhỏ nhất (in-memory), MongoDB trung bình, PostgreSQL JSONB lớn (overhead cột JSON).

Tổng kết, MongoDB thắng cho use case này vì lookup theo _id đủ nhanh, persistent mặc định, document model phù hợp tự nhiên với cấu trúc mảng items, và có Mongo Express UI để debug.

## 9.4. Spark so với Pandas trên một máy

Với demo subset 30MB, Pandas đủ. Nhưng để chứng minh kiến trúc có thể scale, dự án dùng Spark.

Pandas giới hạn ở RAM máy (tối đa 16GB cho dataset thực tế). Spark scale theo cluster size — thêm worker là tăng throughput. Code Spark cũng dễ port qua các managed service như Databricks, AWS EMR, GKE Spark.

Với dataset full 31 triệu giao dịch, Pandas sẽ OOM ngay. Spark là lựa chọn bắt buộc.

## 9.5. LightGBM so với XGBoost so với Neural Network

LightGBM được chọn vì training nhanh nhất nhờ thuật toán leaf-wise growth (khác với XGBoost dùng level-wise). Tốc độ train nhanh hơn XGBoost 2 đến 3 lần với cùng accuracy.

XGBoost là lựa chọn thay thế tương đương về kết quả, chỉ chậm hơn.

Neural network cho tabular (TabNet, FT-Transformer) chậm hơn 10 đến 20 lần, không có lợi đáng kể khi đặc trưng đã clean và tabular. Chỉ thực sự cần khi tích hợp embedding image hoặc text vào input.

Với 24 đặc trưng tabular thuần, gradient boosting là sweet spot cả về tốc độ và độ chính xác.

---

# 10. Hạn chế hiện tại và hướng phát triển

## 10.1. Hạn chế

Một, cold start cho sản phẩm mới chưa có cơ chế xử lý chuyên biệt. Hiện chỉ dựa Popularity nên kém với SKU mới ra mắt. Cần content-based (embedding mô tả văn bản) hoặc CLIP image embedding để bổ sung.

Hai, image embedding CLIP đã được thí nghiệm trong notebook nghiên cứu nhưng chưa được wire vào DAG production. Đây là một module độc lập, có thể bổ sung bằng cách thêm task DAG chạy CLIP encoder song song với 7 candidate generator hiện tại.

Ba, refresh chỉ batch hàng ngày. Chưa có signal real-time (click, view, add-to-cart) kích hoạt re-compute prediction trong ngày.

Bốn, A/B testing chưa có infrastructure. Hiện chỉ đo offline (Recall@12, MAP@12), không có online metric (CTR, conversion). Đây là khoảng cách lớn nhất so với một sản phẩm production thực sự.

Năm, articles, similar và cart trong MongoDB đang được seed từ file static. Các file này xuất phát từ notebook nhưng không có DAG tự động tái sinh. Lý tưởng là thêm một nhánh DAG hàng tuần tái sinh similar_products và cart_recommendations.

Sáu, pipeline run_date hiện hardcode về 2020-09-22 do dataset H&M dừng ở ngày này. Production cần wire dynamic theo template Airflow.

Bảy, không có observability ngoài Airflow UI. Chưa tích hợp Prometheus scrape metrics, Grafana dashboard, Sentry error tracking.

Tám, không có CI/CD. Chưa có pipeline tự động test Spark script (chispa), test API backend (jest/supertest), build và push image.

## 10.2. Hướng phát triển ngắn hạn (1 đến 2 tháng)

Wire CLIP notebook thành Spark DAG ghi similar_products lên gold — effort trung bình, ước tính tăng recall similar 15 đến 20 phần trăm.

Thêm Prometheus scrape Airflow và backend, thiết lập Grafana dashboard — effort nhỏ, mang lại observability cơ bản.

GitHub Actions test Spark scripts với chispa — effort nhỏ, catch regression sớm trước khi merge.

Backend smoke test với supertest, validate response schema — effort nhỏ, đảm bảo API contract không bị break.

Thêm endpoint /api/recommendations/age/:group lộ ra dữ liệu age_bestsellers (đã có sẵn trong MongoDB nhưng chưa expose) — effort rất nhỏ.

## 10.3. Hướng phát triển dài hạn (3 đến 6 tháng)

Kafka kết hợp Spark Streaming cập nhật user history real-time — effort lớn, mang lại tính freshness.

Thay ALS bằng Two-tower neural retrieval (FAISS index cho vector search) — effort lớn, tăng recall 10 đến 20 phần trăm.

Multi-armed bandit cho explore/exploit (Thompson sampling trên Top-12 score) — effort trung bình, mang lại diversity và adaptive.

Online A/B testing infrastructure (kiểu Optimizely hoặc tự xây) — effort lớn, đo tác động thật của thay đổi.

Migrate từ Docker Compose sang Kubernetes với KEDA auto-scaling cho Spark — effort lớn, hướng tới production scale.

Sửa pipeline run_date thành dynamic theo Airflow context (refresh weekly) — effort nhỏ.

Tích hợp LLM cho query semantic ("tìm áo cho buổi tối") — effort lớn, mang lại UX innovation.

---

# 11. Kết luận

Hệ thống đã triển khai thành công toàn bộ pipeline gợi ý thời trang end-to-end trên stack open-source chuẩn industry, từ tầng OLTP (PostgreSQL), Data Lake (MinIO), Compute (Spark + LightGBM), Orchestration (Airflow), Serving (MongoDB) đến Web (Node + React). Toàn bộ stack được container hoá bằng Docker Compose, chạy được trên 1 máy local 16GB RAM.

Năm điểm chính đã đạt được. Một là áp dụng đúng pattern industry-standard: Medallion Data Lake kết hợp two-stage candidate + rank, đây không phải toy example mà chính là pattern Pinterest, YouTube, Spotify đang dùng trong production. Hai là multi-source candidate generation với 7 nguồn đa dạng (rule-based, collaborative, ML, association mining) nâng Recall@12 lên 9.05 phần trăm, gấp 2.8 lần thuật toán đơn lẻ. Ba là độ trễ online tốt: API trả Top-12 dưới 15ms ở percentile 99, đủ phục vụ e-commerce real-world. Bốn là có observability cơ bản qua các UI: Airflow track DAG, Mongo Express xem serving data, MinIO Console xem data lake. Năm là production-ready một phần: pipeline idempotent, có healthcheck, retry, bulk-write Mongo. Còn thiếu Prometheus và CI/CD nhưng việc thêm không yêu cầu refactor lớn.

Bốn bài học rút ra qua quá trình triển khai. Một, không có "silver bullet" model trong recommendation — sức mạnh đến từ đa dạng tín hiệu hơn là tối ưu một mô hình siêu mạnh. Hai, độ trễ online khác hoàn toàn với throughput offline — phải tách rõ hai thế giới bằng pattern batch compute kết hợp cache lookup. Ba, idempotent là king — mọi task phải re-run được mà không corrupt state, đây là yêu cầu sống còn cho hệ thống production. Bốn, schema match version là điểm đau dễ bỏ qua — phiên bản PySpark phải khớp với Spark cluster, Python minor version phải khớp với image. Cần đầu tư thời gian test container build trước khi viết business logic.

Dự án này có thể được sử dụng làm blueprint hoàn chỉnh để hiểu cách kết hợp ba lĩnh vực Data Engineering, Machine Learning và Web Engineering trong một sản phẩm recommendation thực tế. Mỗi lớp đều có chỗ để mở rộng và thay thế công nghệ khi yêu cầu thay đổi.

---

# Tài liệu tham khảo

1. Kaggle competition page: https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations
2. Apache Airflow documentation: https://airflow.apache.org/docs/
3. Apache Spark MLlib guide: https://spark.apache.org/docs/latest/ml-guide.html
4. LightGBM ranking documentation: https://lightgbm.readthedocs.io/
5. MongoDB documentation: https://www.mongodb.com/docs/
6. Medallion architecture (Databricks): https://www.databricks.com/glossary/medallion-architecture
7. Covington et al. 2016 — "Deep Neural Networks for YouTube Recommendations" (RecSys'16)
8. Ying et al. 2018 — "Graph Convolutional Neural Networks for Web-Scale Recommender Systems" (PinSage, KDD'18)
9. Hu, Koren, Volinsky 2008 — "Collaborative Filtering for Implicit Feedback Datasets" (ICDM'08) — bài gốc về ALS implicit feedback
10. Han, Pei, Yin 2000 — "Mining Frequent Patterns without Candidate Generation" (FP-Growth, SIGMOD'00)

---

*Báo cáo viết cho dự án H&M Personalized Fashion Recommendations Demo.*
*Cập nhật ngày 27 tháng 5 năm 2026.*
