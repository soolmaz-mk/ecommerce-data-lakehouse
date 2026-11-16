# پروژه‌ی پایانی — Data Lakehouse

معماری **Medallion** روی Docker Compose: از Kafka تا داشبورد، به‌صورت کامل خودکار و زمان‌بندی‌شده.

```
Kafka  →  Bronze (Spark)  →  Silver (Polars + Iceberg)  →  Gold (dbt + ClickHouse)  →  Metabase
          MinIO/Parquet        Kimball Star                    OBT                      داشبورد
```

---

## لایه‌ی Bronze — ورود داده

- **Spark Structured Streaming** روی دو تاپیک `transactional.orders` و `behavioral.events`
- خواندن **Avro** با **Schema Registry** و نوشتن **Parquet** در MinIO (باکت `bronze/`)
- ساختار ماژولار: `kafka_reader` / `transformation` / `minio_writer` + checkpointing برای تحمل خطا

## لایه‌ی Silver — مدل ابعادی

- پردازش با **Polars + PyIceberg** (بدون Spark — سبک و سریع)
- **ستاره‌ی Kimball**: ۹ جدول بُعد (`dim_user`, `dim_product`, `dim_date`, …) و ۴ جدول واقعیت (`fact_sales`, `fact_orders`, `fact_clickstream`, `fact_returns`)
- الگوی **WAP** (Write-Audit-Publish) با برنچ‌های Iceberg: داده اول در برنچ نوشته، بعد تست کیفیت می‌شود و تنها در صورت قبولی publish می‌شود
- کاتالوگ **Iceberg REST** با پشتیبان PostgreSQL + جاب نگهداری (compaction / snapshot expiry)

## لایه‌ی Gold — مدل تحلیلی

- **dbt** روی **ClickHouse**، خواندن Silver از طریق **Iceberg table engine**
- ۴ جدول **OBT** (One Big Table) بدون نیاز به join در زمان کوئری
- به‌همراه `dbt test` برای اعتبارسنجی خروجی

## لایه‌ی ارائه — Metabase

- **۱۷ کارت** در **۳ داشبورد** با روایت تحلیلی: درآمد و تخفیف / سفر مشتری / بازگشت و وفاداری
- داشبوردها **از روی کد** ساخته می‌شوند (`metabase/questions/*.sql` + اسکریپت idempotent) — قابل بازتولید، نه دستی

## ارکستراسیون — Airflow

| DAG | زمان‌بندی | کار |
| --- | --- | --- |
| `silver_load` | ساعتی (`:00`) | Bronze → ستاره‌ی Silver |
| `gold_dbt_load` | ساعتی (`:15`) | اجرای مدل‌های dbt |
| `silver_maintenance` | دوره‌ای | نگهداری جداول Iceberg |

وابستگی بین DAGها با **ExternalTaskSensor** کنترل می‌شود.

---

## نتیجه

خط لوله‌ی **end-to-end** و پایدار؛ آخرین اجرا با موفقیت کامل:

| جدول | تعداد رکورد |
| --- | --- |
| `obt_clickstream` | ۱٬۸۸۰٬۷۹۱ |
| `obt_sales` | ۵۰٬۴۴۷ |
| `obt_orders` | ۳۹٬۶۸۹ |
| `obt_returns` | ۵٬۲۲۴ |

## چالش‌های فنی حل‌شده

- **ناهم‌ترازی زمانی DAGها:** `gold` در دقیقه‌ی `:15` اجرا می‌شد ولی سنسور دنبال اجرای `silver` در همان دقیقه می‌گشت؛ با `execution_delta` برطرف شد
- **باگ آنالایزر ClickHouse:** روی جداول خارجی Iceberg، مدل‌ها به‌جای CTE با `SELECT` تخت نوشته شدند
- **مصرف حافظه:** تسک‌های dbt به‌صورت ترتیبی اجرا می‌شوند تا سرور دچار کمبود RAM نشود
- **پایداری کاتالوگ:** جایگزینی SQLite پیش‌فرض ایمیج Iceberg با PostgreSQL تا تعاریف جداول از بین نرود
