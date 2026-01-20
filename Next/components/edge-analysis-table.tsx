'use client';

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';
import type { EdgeAnalysis } from '@/lib/types';

interface EdgeAnalysisTableProps {
  data: EdgeAnalysis[];
  className?: string;
}

export function EdgeAnalysisTable({ data, className }: EdgeAnalysisTableProps) {
  const formatPercent = (value: number) => `${(value * 100).toFixed(2)}%`;
  const formatOdds = (value: number) => value.toFixed(2);
  const formatCurrency = (value: number) => `€${value.toFixed(2)}`;

  if (!data || data.length === 0) {
    return (
      <div className={cn('rounded-lg border border-border bg-card p-6 text-center', className)}>
        <p className="text-muted-foreground">Nessuna opportunita con edge positivo trovata</p>
      </div>
    );
  }

  // Sort by edge descending
  const sortedData = [...data].sort((a, b) => b.edge - a.edge);

  return (
    <div className={cn('rounded-lg border border-border bg-card overflow-hidden', className)}>
      <div className="border-b border-border px-4 py-3">
        <h3 className="text-sm font-medium">Analisi Edge & Expected Value</h3>
        <p className="text-xs text-muted-foreground mt-1">
          Opportunita ordinate per edge decrescente
        </p>
      </div>
      <div className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow className="hover:bg-transparent">
              <TableHead className="text-muted-foreground">Mercato</TableHead>
              <TableHead className="text-muted-foreground">Esito</TableHead>
              <TableHead className="text-right text-muted-foreground">Fair Prob</TableHead>
              <TableHead className="text-right text-muted-foreground">Market Prob</TableHead>
              <TableHead className="text-right text-muted-foreground">Edge</TableHead>
              <TableHead className="text-right text-muted-foreground">EV</TableHead>
              <TableHead className="text-right text-muted-foreground">Kelly</TableHead>
              <TableHead className="text-right text-muted-foreground">Stake</TableHead>
              <TableHead className="text-center text-muted-foreground">CI</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {sortedData.map((row, idx) => (
              <TableRow key={idx} className="hover:bg-secondary/50">
                <TableCell className="font-medium">{row.market}</TableCell>
                <TableCell>{row.outcome}</TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {formatPercent(row.fair_prob)}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {formatPercent(row.market_prob)}
                </TableCell>
                <TableCell
                  className={cn(
                    'text-right font-mono tabular-nums font-medium',
                    row.edge > 0.05 ? 'text-success' : row.edge > 0 ? 'text-warning' : 'text-destructive'
                  )}
                >
                  {row.edge > 0 ? '+' : ''}
                  {formatPercent(row.edge)}
                </TableCell>
                <TableCell
                  className={cn(
                    'text-right font-mono tabular-nums',
                    row.ev > 0 ? 'text-success' : 'text-destructive'
                  )}
                >
                  {row.ev > 0 ? '+' : ''}
                  {formatPercent(row.ev)}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {formatPercent(row.kelly_fraction)}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums font-medium text-primary">
                  {formatCurrency(row.recommended_stake)}
                </TableCell>
                <TableCell className="text-center">
                  <Badge variant="outline" className="text-xs font-mono">
                    {formatPercent(row.confidence_interval[0])} - {formatPercent(row.confidence_interval[1])}
                  </Badge>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
