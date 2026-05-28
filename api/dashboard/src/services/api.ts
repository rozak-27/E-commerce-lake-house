/**
 * API Service
 * Axios client untuk komunikasi ke FastAPI
 */
import axios from 'axios';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export const api = axios.create({
  baseURL: API_BASE_URL,
  headers: { 'Content-Type': 'application/json' },
});

// ── Types ─────────────────────────────────────────────────
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

// ── API Calls ─────────────────────────────────────────────
export const getRevenue = async (): Promise<RevenueData[]> => {
  const res = await api.get('/api/revenue');
  return res.data.data;
};

export const getDau = async (): Promise<DauData[]> => {
  const res = await api.get('/api/dau');
  return res.data.data;
};

export const getTopProducts = async (limit = 10): Promise<TopProduct[]> => {
  const res = await api.get(`/api/top-products?limit=${limit}`);
  return res.data.data;
};

export const getFunnel = async (): Promise<FunnelData[]> => {
  const res = await api.get('/api/funnel');
  return res.data.data;
};

export const getPayments = async (): Promise<PaymentData[]> => {
  const res = await api.get('/api/payments');
  return res.data.data;
};