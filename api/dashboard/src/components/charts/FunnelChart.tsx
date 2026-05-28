/**
 * Funnel Chart
 * Horizontal bar chart konversi view → add_to_cart → checkout
 */
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell } from 'recharts';
import { useFunnel } from '../../hooks/useAnalytics';

const COLORS: Record<string, string> = {
  view:        '#6366f1',
  search:      '#8b5cf6',
  add_to_cart: '#f59e0b',
  checkout:    '#10b981',
  review:      '#3b82f6',
};

export default function FunnelChart() {
  const { data, isLoading, error } = useFunnel();

  if (isLoading) return <div className="flex items-center justify-center h-64 text-gray-400">Loading...</div>;
  if (error)     return <div className="flex items-center justify-center h-64 text-red-400">Error loading data</div>;

  return (
    <div className="bg-white rounded-xl p-6 shadow-sm border border-gray-100">
      <h2 className="text-lg font-semibold text-gray-800 mb-4">Funnel Konversi</h2>
      <ResponsiveContainer width="100%" height={280}>
        <BarChart data={data} layout="vertical">
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis type="number" tick={{ fontSize: 12 }} />
          <YAxis dataKey="event_type" type="category" tick={{ fontSize: 12 }} width={90} />
          <Tooltip formatter={(value: number) => [value.toLocaleString('id-ID'), 'Events']} />
          <Bar dataKey="total_events" radius={[0, 4, 4, 0]}>
            {data.map((entry) => (
              <Cell key={entry.event_type} fill={COLORS[entry.event_type] ?? '#6366f1'} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}