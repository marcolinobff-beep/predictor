'use client';

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { cn } from '@/lib/utils';
import type { Probability } from '@/lib/types';

interface ProbabilityTableProps {
  data: Probability[];
  title?: string;
  showEdge?: boolean;
  className?: string;
}

export function ProbabilityTable({
  data,
  title,
  showEdge = true,
  className,
}: ProbabilityTableProps) {
  const formatPercent = (value: number) => `${(value * 100).toFixed(1)}%`;
  const formatOdds = (value: number) => value.toFixed(2);
  const formatEdge = (value: number | undefined) => {
    if (value === undefined) return '-';
    const percent = (value * 100).toFixed(1);
    return value >= 0 ? `+${percent}%` : `${percent}%`;
  };

  return (
    <div className={cn('rounded-lg border border-border bg-card', className)}>
      {title && (
        <div className="border-b border-border px-4 py-3">
          <h3 className="text-sm font-medium">{title}</h3>
        </div>
      )}
      <Table>
        <TableHeader>
          <TableRow className="hover:bg-transparent">
            <TableHead className="text-muted-foreground">Esito</TableHead>
            <TableHead className="text-right text-muted-foreground">Prob.</TableHead>
            <TableHead className="text-right text-muted-foreground">Fair Odds</TableHead>
            {showEdge && (
              <>
                <TableHead className="text-right text-muted-foreground">Quota</TableHead>
                <TableHead className="text-right text-muted-foreground">Edge</TableHead>
              </>
            )}
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.map((row) => (
            <TableRow key={row.outcome} className="hover:bg-secondary/50">
              <TableCell className="font-medium">{row.outcome}</TableCell>
              <TableCell className="text-right font-mono tabular-nums">
                {formatPercent(row.probability)}
              </TableCell>
              <TableCell className="text-right font-mono tabular-nums">
                {formatOdds(row.fair_odds)}
              </TableCell>
              {showEdge && (
                <>
                  <TableCell className="text-right font-mono tabular-nums">
                    {row.market_odds ? formatOdds(row.market_odds) : '-'}
                  </TableCell>
                  <TableCell
                    className={cn(
                      'text-right font-mono tabular-nums',
                      row.edge && row.edge > 0 ? 'text-success' : 'text-muted-foreground'
                    )}
                  >
                    {formatEdge(row.edge)}
                  </TableCell>
                </>
              )}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}
