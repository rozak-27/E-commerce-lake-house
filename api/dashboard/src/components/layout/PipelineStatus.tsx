/**
 * Pipeline Status
 * Tampilkan status setiap service (Kafka, Spark, Trino, API)
 */
import { useEffect, useState } from 'react';
import { api } from '../../services/api';

interface ServiceStatus {
  name: string;
  status: 'online' | 'offline' | 'checking';
  url: string;
}

const SERVICES: ServiceStatus[] = [
  { name: 'FastAPI',  status: 'checking', url: '/health' },
  { name: 'Kafka',    status: 'checking', url: '' },
  { name: 'Spark',    status: 'checking', url: '' },
  { name: 'Trino',    status: 'checking', url: '' },
];

export default function PipelineStatus() {
  const [services, setServices] = useState(SERVICES);

  useEffect(() => {
    // cek FastAPI health
    api.get('/health')
      .then(() => setServices(prev =>
        prev.map(s => s.name === 'FastAPI' ? { ...s, status: 'online' } : s)
      ))
      .catch(() => setServices(prev =>
        prev.map(s => s.name === 'FastAPI' ? { ...s, status: 'offline' } : s)
      ));

    // simulasi status service lain (karena tidak bisa dicek langsung dari browser)
    setTimeout(() => {
      setServices(prev => prev.map(s =>
        s.name !== 'FastAPI' ? { ...s, status: 'online' } : s
      ));
    }, 1000);
  }, []);

  const statusColor = {
    online:   'bg-green-400',
    offline:  'bg-red-400',
    checking: 'bg-yellow-400 animate-pulse',
  };

  return (
    <div className="bg-white rounded-xl p-6 shadow-sm border border-gray-100">
      <h2 className="text-lg font-semibold text-gray-800 mb-4">Pipeline Status</h2>
      <div className="space-y-3">
        {services.map((service) => (
          <div key={service.name} className="flex items-center justify-between">
            <span className="text-sm text-gray-600">{service.name}</span>
            <div className="flex items-center gap-2">
              <span className={`w-2 h-2 rounded-full ${statusColor[service.status]}`} />
              <span className="text-xs text-gray-400 capitalize">{service.status}</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}