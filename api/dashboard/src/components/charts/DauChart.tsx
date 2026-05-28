/**
 * DAU Chart
 * Bar chart Daily Active Users
 */
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { useDau } from '../../hooks/useAnalytics';

const formatDate = (dateStr: string) =>
  new Date(dateStr).toLocaleDateString('id-ID', { day: '2-digit', month: 'short' });

export default function DauChart() {
  const { data, isLoading, error } = useDau();

  if (isLoading) return <div className="flex items-center justify-center h-64 text-gray-400">Loading...</div>;
  if (error)     return <div className="flex items-center justify-center h-64 text-red-400">Error loading data</div>;

  return (
    <div className="bg-white rounded-xl p-6 shadow-sm border border-gray-100">
      <h2 className="text-lg font-semibold text-gray-800 mb-4">Daily Active Users</h2>
      <ResponsiveContainer width="100%" height={280}>
        <BarChart data={[...data].reverse()}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis dataKey="date" tickFormatter={formatDate} tick={{ fontSize: 12 }} />
          <YAxis tick={{ fontSize: 12 }} />
          <Tooltip
            formatter={(value: number) => [value.toLocaleString('id-ID'), 'Active Users']}
            labelFormatter={(label) => formatDate(label)}
          />
          <Bar dataKey="active_users" fill="#10b981" radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}