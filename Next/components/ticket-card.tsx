'use client';

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';
import type { Ticket } from '@/lib/types';
import { Target, TrendingUp, Zap } from 'lucide-react';

interface TicketCardProps {
  ticket: Ticket;
  className?: string;
}

const difficultyConfig = {
  easy: {
    label: 'Facile',
    icon: Target,
    color: 'bg-success/10 text-success border-success/20',
  },
  medium: {
    label: 'Media',
    icon: TrendingUp,
    color: 'bg-warning/10 text-warning border-warning/20',
  },
  hard: {
    label: 'Difficile',
    icon: Zap,
    color: 'bg-destructive/10 text-destructive border-destructive/20',
  },
};

export function TicketCard({ ticket, className }: TicketCardProps) {
  const config = difficultyConfig[ticket.difficulty];
  const Icon = config.icon;

  const formatOdds = (value: number) => value.toFixed(2);
  const formatPercent = (value: number) => `${(value * 100).toFixed(1)}%`;
  const formatCurrency = (value: number) => `€${value.toFixed(2)}`;

  return (
    <Card className={cn('bg-card border-border', className)}>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-base flex items-center gap-2">
            <Icon className="h-4 w-4" />
            {ticket.name}
          </CardTitle>
          <Badge variant="outline" className={cn('text-xs', config.color)}>
            {config.label}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Selections */}
        <div className="space-y-2">
          {ticket.selections.map((selection, idx) => (
            <div
              key={idx}
              className="rounded-md bg-secondary/50 px-3 py-2 text-sm"
            >
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground truncate max-w-[60%]">
                  {selection.match}
                </span>
                <span className="font-mono tabular-nums">
                  @{formatOdds(selection.odds)}
                </span>
              </div>
              <div className="flex items-center justify-between mt-1">
                <span className="font-medium">
                  {selection.market}: {selection.outcome}
                </span>
                <span
                  className={cn(
                    'text-xs font-mono',
                    selection.edge > 0 ? 'text-success' : 'text-muted-foreground'
                  )}
                >
                  {selection.edge > 0 ? '+' : ''}
                  {(selection.edge * 100).toFixed(1)}%
                </span>
              </div>
            </div>
          ))}
        </div>

        {/* Summary */}
        <div className="rounded-md border border-border p-3 space-y-2">
          <div className="flex items-center justify-between text-sm">
            <span className="text-muted-foreground">Quota Totale</span>
            <span className="font-bold font-mono tabular-nums text-lg">
              {formatOdds(ticket.combined_odds)}
            </span>
          </div>
          <div className="flex items-center justify-between text-sm">
            <span className="text-muted-foreground">Probabilita</span>
            <span className="font-mono tabular-nums">
              {formatPercent(ticket.combined_probability)}
            </span>
          </div>
          <div className="flex items-center justify-between text-sm">
            <span className="text-muted-foreground">EV Atteso</span>
            <span
              className={cn(
                'font-mono tabular-nums',
                ticket.expected_value > 0 ? 'text-success' : 'text-destructive'
              )}
            >
              {ticket.expected_value > 0 ? '+' : ''}
              {(ticket.expected_value * 100).toFixed(1)}%
            </span>
          </div>
          <div className="pt-2 border-t border-border">
            <div className="flex items-center justify-between">
              <span className="text-muted-foreground text-sm">Stake Suggerito</span>
              <span className="font-bold font-mono tabular-nums text-primary">
                {formatCurrency(ticket.suggested_stake)}
              </span>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
