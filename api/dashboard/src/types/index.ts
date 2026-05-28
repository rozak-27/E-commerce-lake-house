/**
 * Types index
 */
export interface RevenueData {
  date: string;
  total_revenue: number;
  total_orders: number;
  unique_buyers: number;
}

export interface DauData {
  date: string;
  active_users: number;
  total_events: number;
}

export interface TopProduct {
  product_id: string;
  product_name: string;
  category: string;
  total_revenue: number;
  total_quantity: number;
  total_orders: number;
}

export interface FunnelData {
  event_type: string;
  total_events: number;
  total_users: number;
}

export interface PaymentData {
  payment_method: string;
  status: string;
  transaction_count: number;
  total_amount: number;
}