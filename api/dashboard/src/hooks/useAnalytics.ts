/**
 * useAnalytics Hook
 * SWR hooks dengan auto-refresh setiap 30 detik
 */
import useSWR from 'swr';
import { getRevenue, getDau, getTopProducts, getFunnel, getPayments } from '../services/api';

const REFRESH_INTERVAL = 30_000; // 30 detik

export const useRevenue = () => {
  const { data, error, isLoading } = useSWR('revenue', getRevenue, {
    refreshInterval: REFRESH_INTERVAL,
  });
  return { data: data ?? [], error, isLoading };
};

export const useDau = () => {
  const { data, error, isLoading } = useSWR('dau', getDau, {
    refreshInterval: REFRESH_INTERVAL,
  });
  return { data: data ?? [], error, isLoading };
};

export const useTopProducts = (limit = 10) => {
  const { data, error, isLoading } = useSWR(
    ['top-products', limit],
    () => getTopProducts(limit),
    { refreshInterval: REFRESH_INTERVAL }
  );
  return { data: data ?? [], error, isLoading };
};

export const useFunnel = () => {
  const { data, error, isLoading } = useSWR('funnel', getFunnel, {
    refreshInterval: REFRESH_INTERVAL,
  });
  return { data: data ?? [], error, isLoading };
};

export const usePayments = () => {
  const { data, error, isLoading } = useSWR('payments', getPayments, {
    refreshInterval: REFRESH_INTERVAL,
  });
  return { data: data ?? [], error, isLoading };
};