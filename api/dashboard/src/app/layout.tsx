import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'E-Commerce Lakehouse Dashboard',
  description: 'Analytics dashboard untuk e-commerce lakehouse',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="id">
      <body>{children}</body>
    </html>
  );
}