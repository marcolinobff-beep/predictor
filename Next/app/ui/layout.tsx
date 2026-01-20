'use client';

import React from "react"

import { useEffect, useState } from 'react';
import { useRouter, usePathname } from 'next/navigation';
import { isAuthenticated } from '@/lib/api';
import { AppSidebar } from '@/components/app-sidebar';
import { LoadingState } from '@/components/states';

export default function UILayout({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const pathname = usePathname();
  const [isChecking, setIsChecking] = useState(true);

  useEffect(() => {
    // Skip auth check for login page
    if (pathname === '/ui/login') {
      setIsChecking(false);
      return;
    }

    if (!isAuthenticated()) {
      router.replace('/ui/login');
    } else {
      setIsChecking(false);
    }
  }, [pathname, router]);

  // Show loading while checking auth
  if (isChecking && pathname !== '/ui/login') {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingState message="Verifica autenticazione..." />
      </div>
    );
  }

  // Login page without sidebar
  if (pathname === '/ui/login') {
    return <>{children}</>;
  }

  // Main layout with sidebar
  return (
    <div className="min-h-screen bg-background">
      <AppSidebar />
      <main className="pl-64">
        <div className="p-6">{children}</div>
      </main>
    </div>
  );
}
