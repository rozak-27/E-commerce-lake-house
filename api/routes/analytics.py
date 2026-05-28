"""
Analytics Routes
================
Endpoint:
  GET /revenue      → revenue harian
  GET /dau          → daily active users
  GET /top-products → produk terlaris
  GET /funnel       → funnel konversi
  GET /payments     → summary pembayaran
"""
from fastapi import APIRouter, HTTPException
from services.trino_service import execute_query

router = APIRouter(prefix="/api", tags=["analytics"])

@router.get("/revenue")
def get_revenue():
    """Total revenue per hari"""
    try:
        data = execute_query("""
            SELECT
                date,
                total_revenue,
                total_orders,
                unique_buyers
            FROM iceberg.gold.revenue_daily
            ORDER BY date DESC
            LIMIT 30
        """)
        return {"status": "ok", "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dau")
def get_dau():
    """Daily Active Users per hari"""
    try:
        data = execute_query("""
            SELECT
                date,
                active_users,
                total_events
            FROM iceberg.gold.dau_daily
            ORDER BY date DESC
            LIMIT 30
        """)
        return {"status": "ok", "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/top-products")
def get_top_products(limit: int = 10):
    """Produk terlaris by revenue"""
    try:
        data = execute_query(f"""
            SELECT
                product_id,
                product_name,
                category,
                total_revenue,
                total_quantity,
                total_orders
            FROM iceberg.gold.top_products
            ORDER BY total_revenue DESC
            LIMIT {limit}
        """)
        return {"status": "ok", "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/funnel")
def get_funnel():
    """Funnel konversi: view → add_to_cart → checkout"""
    try:
        data = execute_query("""
            SELECT
                event_type,
                SUM(event_count)   AS total_events,
                SUM(unique_users)  AS total_users
            FROM iceberg.gold.funnel_events
            GROUP BY event_type
            ORDER BY total_events DESC
        """)
        return {"status": "ok", "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/payments")
def get_payments():
    """Summary pembayaran per metode dan status"""
    try:
        data = execute_query("""
            SELECT
                payment_method,
                status,
                transaction_count,
                total_amount
            FROM iceberg.gold.payment_summary
            ORDER BY transaction_count DESC
        """)
        return {"status": "ok", "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))