'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { cn } from '@/lib/utils';
import { Button } from '@/components/ui/button';
import { removeToken } from '@/lib/api';
import {
  LayoutDashboard,
  Search,
  Calendar,
  TrendingUp,
  Users,
  FileText,
  LogOut,
  Activity,
} from 'lucide-react';

const navItems = [
  { href: '/ui', label: 'Dashboard', icon: LayoutDashboard },
  { href: '/ui/analyze', label: 'Analisi Match', icon: Search },
  { href: '/ui/slate', label: 'Pronostici Giornata', icon: Calendar },
  { href: '/ui/odds-risk', label: 'Quote & Rischio', icon: TrendingUp },
  { href: '/ui/players', label: 'Proiezioni Giocatori', icon: Users },
  { href: '/ui/audit', label: 'Storico Audit', icon: FileText },
];

export function AppSidebar() {
  const pathname = usePathname();

  const handleLogout = () => {
    removeToken();
    window.location.href = '/ui/login';
  };

  return (
    <aside className="fixed left-0 top-0 z-40 h-screen w-64 border-r border-sidebar-border bg-sidebar">
      <div className="flex h-full flex-col">
        {/* Logo */}
        <div className="flex h-16 items-center gap-2 border-b border-sidebar-border px-6">
          <Activity className="h-6 w-6 text-primary" />
          <div>
            <h1 className="text-lg font-semibold text-sidebar-foreground">
              Football Bot
            </h1>
            <p className="text-xs text-muted-foreground">Prediction Engine</p>
          </div>
        </div>

        {/* Navigation */}
        <nav className="flex-1 space-y-1 px-3 py-4">
          {navItems.map((item) => {
            const isActive = pathname === item.href;
            const Icon = item.icon;

            return (
              <Link
                key={item.href}
                href={item.href}
                className={cn(
                  'flex items-center gap-3 rounded-md px-3 py-2.5 text-sm font-medium transition-colors',
                  isActive
                    ? 'bg-sidebar-accent text-sidebar-primary'
                    : 'text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground'
                )}
              >
                <Icon className="h-4 w-4" />
                {item.label}
              </Link>
            );
          })}
        </nav>

        {/* Footer */}
        <div className="border-t border-sidebar-border p-4">
          <Button
            variant="ghost"
            className="w-full justify-start gap-3 text-muted-foreground hover:text-destructive"
            onClick={handleLogout}
          >
            <LogOut className="h-4 w-4" />
            Esci
          </Button>
        </div>
      </div>
    </aside>
  );
}
