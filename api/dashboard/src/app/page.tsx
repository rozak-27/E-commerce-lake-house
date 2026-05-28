/**
 * Dashboard Page
 * Halaman utama analytics dashboard
 */
'use client';

import { useRevenue, useDau, useTopProducts } from '../hooks/useAnalytics';
import RevenueChart  from '../components/charts/RevenueChart';
import DauChart      from '../components/charts/DauChart';
import FunnelChart   from '../components/charts/FunnelChart';
import MetricCard    from '../components/layout/MetricCard';
import PipelineStatus from '../components/layout/PipelineStatus';

const formatRupiah = (value: number) =>
  new Intl.NumberFormat('id-ID', { style: 'currency', currency: 'IDR', notation: 'compact' }).format(value);

export default function Dashboard() {
  const { data: revenue }  = useRevenue();
  const { data: dau }      = useDau();
  const { data: products } = useTopProducts(5);

  const latestRevenue = revenue[0];
  const latestDau     = dau[0];

  return (
    <main className="min-h-screen bg-gray-50 p-6">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-900">E-Commerce Analytics</h1>
      </div>

      {/* Metric Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
        <MetricCard
          title="Total Revenue"
          value={latestRevenue ? formatRupiah(latestRevenue.total_revenue) : '-'}
          subtitle="Hari ini"
          color="indigo"
        />
        <MetricCard
          title="Total Orders"
          value={latestRevenue ? latestRevenue.total_orders.toLocaleString('id-ID') : '-'}
          subtitle="Hari ini"
          color="green"
        />
        <MetricCard
          title="Active Users"
          value={latestDau ? latestDau.active_users.toLocaleString('id-ID') : '-'}
          subtitle="Hari ini"
          color="blue"
        />
        <MetricCard
          title="Unique Buyers"
          value={latestRevenue ? latestRevenue.unique_buyers.toLocaleString('id-ID') : '-'}
          subtitle="Hari ini"
          color="amber"
        />
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
        <RevenueChart />
        <DauChart />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {/* Funnel */}
        <div className="md:col-span-2">
          <FunnelChart />
        </div>

        {/* Top Products + Pipeline Status */}
        <div className="space-y-6">
          <div className="bg-white rounded-xl p-6 shadow-sm border border-gray-100">
            <h2 className="text-lg font-semibold text-gray-800 mb-4">Top Produk</h2>
            <div className="space-y-3">
              {products.map((p, i) => (
                <div key={p.product_id} className="flex items-center gap-3">
                  <span className="text-sm font-bold text-gray-400 w-4">{i + 1}</span>
                  <div className="flex-1 min-w-0">
                    <div className="text-sm font-medium text-gray-700 truncate">{p.product_name}</div>
                    <div className="text-xs text-gray-400">{p.category}</div>
                  </div>
                  <div className="text-sm font-semibold text-indigo-600">
                    {formatRupiah(p.total_revenue)}
                  </div>
                </div>
              ))}
            </div>
          </div>
          <PipelineStatus />
        </div>
      </div>
    </main>
  );
}