'use client';

import React from "react"

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { setToken } from '@/lib/api';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Activity, Eye, EyeOff, LogIn } from 'lucide-react';

export default function LoginPage() {
  const router = useRouter();
  const [token, setTokenValue] = useState('');
  const [showToken, setShowToken] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');

    if (!token.trim()) {
      setError('Inserisci un token valido');
      return;
    }

    setIsLoading(true);

    // Simulate a small delay for UX
    await new Promise((resolve) => setTimeout(resolve, 500));

    // Store the token
    setToken(token.trim());

    // Redirect to dashboard
    router.push('/ui');
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-background p-4">
      <div className="w-full max-w-md">
        {/* Logo and Title */}
        <div className="text-center mb-8">
          <div className="inline-flex items-center justify-center rounded-full bg-primary/10 p-4 mb-4">
            <Activity className="h-10 w-10 text-primary" />
          </div>
          <h1 className="text-3xl font-bold text-foreground">Football Prediction Bot</h1>
          <p className="text-muted-foreground mt-2">
            Motore locale per analisi partite e suggerimenti di mercato
          </p>
        </div>

        {/* Login Card */}
        <Card className="bg-card border-border">
          <CardHeader>
            <CardTitle>Accesso</CardTitle>
            <CardDescription>
              Inserisci il tuo token API per accedere alla piattaforma
            </CardDescription>
          </CardHeader>
          <CardContent>
            <form onSubmit={handleSubmit} className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="token">Token API</Label>
                <div className="relative">
                  <Input
                    id="token"
                    type={showToken ? 'text' : 'password'}
                    value={token}
                    onChange={(e) => setTokenValue(e.target.value)}
                    placeholder="Inserisci il tuo token..."
                    className="pr-10"
                    autoComplete="off"
                  />
                  <button
                    type="button"
                    onClick={() => setShowToken(!showToken)}
                    className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground transition-colors"
                  >
                    {showToken ? (
                      <EyeOff className="h-4 w-4" />
                    ) : (
                      <Eye className="h-4 w-4" />
                    )}
                  </button>
                </div>
              </div>

              {error && (
                <p className="text-sm text-destructive">{error}</p>
              )}

              <Button
                type="submit"
                className="w-full"
                disabled={isLoading}
              >
                {isLoading ? (
                  <>
                    <span className="animate-spin mr-2">
                      <Activity className="h-4 w-4" />
                    </span>
                    Accesso in corso...
                  </>
                ) : (
                  <>
                    <LogIn className="h-4 w-4 mr-2" />
                    Accedi
                  </>
                )}
              </Button>
            </form>
          </CardContent>
        </Card>

        {/* Footer */}
        <p className="text-center text-xs text-muted-foreground mt-6">
          Serie A/B, EPL, Bundesliga, La Liga, Ligue 1
        </p>
      </div>
    </div>
  );
}
