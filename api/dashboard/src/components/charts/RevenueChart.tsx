/**
 * Revenue Chart
 * Line chart total revenue per hari
 */
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { useRevenue } from '../../hooks/useAnalytics';

const formatRupiah = (value: number) =>
  new Intl.NumberFormat('id-ID', { style: 'currency', currency: 'IDR', notation: 'compact' }).format(value);

const formatDate = (dateStr: string) =>
  new Date(dateStr).toLocaleDateString('id-ID', { day: '2-digit', month: 'short' });

export default function RevenueChart() {
  const { data, isLoading, error } = useRevenue();

  if (isLoading) return <div className="flex items-center justify-center h-64 text-gray-400">Loading...</div>;
  if (error)     return <div className="flex items-center justify-center h-64 text-red-400">Error loading data</div>;

  return (
    <div className="bg-white rounded-xl p-6 shadow-sm border border-gray-100">
      <h2 className="text-lg font-semibold text-gray-800 mb-4">Revenue Harian</h2>
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={[...data].reverse()}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis dataKey="date" tickFormatter={formatDate} tick={{ fontSize: 12 }} />
          <YAxis tickFormatter={formatRupiah} tick={{ fontSize: 12 }} width={80} />
          <Tooltip
            formatter={(value: number) => [formatRupiah(value), 'Revenue']}
            labelFormatter={(label) => formatDate(label)}
          />
          <Line type="monotone" dataKey="total_revenue" stroke="#6366f1" strokeWidth={2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}