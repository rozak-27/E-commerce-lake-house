/**
 * Metric Card
 * Kartu ringkasan metrik (revenue, orders, users, dll)
 */
interface MetricCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  icon?: string;
  color?: 'indigo' | 'green' | 'amber' | 'blue';
}

const colorMap = {
  indigo: 'bg-indigo-50 text-indigo-600',
  green:  'bg-green-50 text-green-600',
  amber:  'bg-amber-50 text-amber-600',
  blue:   'bg-blue-50 text-blue-600',
};

export default function MetricCard({ title, value, subtitle, icon, color = 'indigo' }: MetricCardProps) {
  return (
    <div className="bg-white rounded-xl p-6 shadow-sm border border-gray-100">
      <div className="flex items-center justify-between mb-3">
        <span className="text-sm font-medium text-gray-500">{title}</span>
        {icon && (
          <span className={`text-xl p-2 rounded-lg ${colorMap[color]}`}>{icon}</span>
        )}
      </div>
      <div className="text-2xl font-bold text-gray-800">{value}</div>
      {subtitle && <div className="text-sm text-gray-400 mt-1">{subtitle}</div>}
    </div>
  );
}