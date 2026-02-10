export const SQL_KEYWORDS: string[] = [
  'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING',
  'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN', 'INNER JOIN', 'OUTER JOIN',
  'ON',
  'INSERT', 'INTO', 'VALUES',
  'UPDATE', 'SET',
  'DELETE',
  'DISTINCT', 'AS',
  'AND', 'OR', 'NOT',
  'IN', 'EXISTS', 'LIKE', 'BETWEEN', 'IS', 'NULL',
  'UNION', 'UNION ALL', 'INTERSECT', 'MINUS',
  'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
  'WITH',
  'LIMIT', 'OFFSET',
  'FETCH', 'FIRST', 'ROWS', 'ONLY',
  'OVER', 'PARTITION BY'
];

export const SQL_OPERATORS: string[] = [
  '=', '!=', '<>', '>', '>=', '<', '<=',
  '+', '-', '*', '/', '%',
  '||',
  '(', ')', ',', '.', ';'
];
