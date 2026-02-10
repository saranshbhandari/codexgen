/* eslint-disable @typescript-eslint/no-explicit-any */

export interface TableMeta {
  name: string;
  columns: string[];
}

export interface SchemaMeta {
  name: string;
  tables: TableMeta[];
}

export interface FunctionMeta {
  name: string;
  description: string;
  sample: string;
}

export interface WorkflowVarDTO {
  scope: string;
  variableKey: string;
}

export interface IntellisenseConfig {
  dbType: string;
  metadata: SchemaMeta[];
  dbBuiltins: Record<string, FunctionMeta[]>;
  sqlKeywords: string[];
  sqlOperators: string[];
  workflowVars: WorkflowVarDTO[];
}

export interface CompletionBuildInput {
  monaco: any;
  model: any;
  position: any;
  languageId: string;
  config: IntellisenseConfig;
}

type TableRef = { schema?: string; table: string };

type SqlCtx =
  | 'NONE'
  | 'SELECT'
  | 'FROM'
  | 'JOIN'
  | 'ON'
  | 'WHERE'
  | 'UPDATE'
  | 'SET'
  | 'DELETE'
  | 'INSERT'
  | 'INTO'
  | 'VALUES'
  | 'GROUP'
  | 'ORDER'
  | 'HAVING'
  | 'INSERT_COLS';

export class SqlIntellisenseUtil {
  private schemaLookup = new Map<string, SchemaMeta>(); // upper(schema) -> schema
  private tableLookup = new Map<string, { schema?: string; table: TableMeta }>(); // upper("S.T") or upper("T")
  private variableScopes = new Map<string, string[]>(); // scope -> keys[]

  rebuildLookups(config: IntellisenseConfig): void {
    this.schemaLookup.clear();
    this.tableLookup.clear();

    for (const s of config.metadata ?? []) {
      this.schemaLookup.set(s.name.toUpperCase(), s);
      for (const t of s.tables ?? []) {
        const fq = `${s.name}.${t.name}`.toUpperCase();
        this.tableLookup.set(fq, { schema: s.name, table: t });
        this.tableLookup.set(t.name.toUpperCase(), { schema: s.name, table: t }); // allow no-schema
      }
    }

    this.variableScopes.clear();
    for (const v of config.workflowVars ?? []) {
      const scope = (v.scope ?? '').trim();
      const key = (v.variableKey ?? '').trim();
      if (!scope || !key) continue;

      const arr = this.variableScopes.get(scope) ?? [];
      arr.push(key);
      this.variableScopes.set(scope, arr);
    }

    for (const [scope, arr] of this.variableScopes.entries()) {
      this.variableScopes.set(scope, Array.from(new Set(arr)).sort());
    }
  }

  buildCompletions(input: CompletionBuildInput): { suggestions: any[] } {
    const { monaco, model, position, config } = input;
    const suggestions: any[] = [];

    const textUntil = model.getValueInRange({
      startLineNumber: 1,
      startColumn: 1,
      endLineNumber: position.lineNumber,
      endColumn: position.column
    });

    const word = model.getWordUntilPosition(position);
    const range = {
      startLineNumber: position.lineNumber,
      startColumn: word.startColumn,
      endLineNumber: position.lineNumber,
      endColumn: word.endColumn
    };

    // 1) ${scope.key} workflow vars (highest priority)
    const varCtx = this.detectVariableContext(textUntil);
    if (varCtx.inVar) {
      this.addWorkflowVarSuggestions(monaco, suggestions, range, varCtx);
      return { suggestions: this.dedupeSuggestions(suggestions) };
    }

    // 2) Parentheses-depth aware statement + scope parsing
    const stmt = this.currentStatement(textUntil);
    const stmtStripped = this.stripStrings(stmt);

    const ctx = this.detectContext(textUntil, stmtStripped);
    const scope = this.extractScopeAtCursor(stmtStripped); // depth-aware
    const scopeUpper = scope.toUpperCase();

    const { tableMap, updateTarget, insertTarget } = this.parseTableRefsDepthAware(scopeUpper);

    // 3) Dot-intellisense (alias./schema./schema.table.)
    const token = this.lastToken(textUntil);
    const parts = token.split('.').filter(Boolean);

    if (parts.length === 1 && token.endsWith('.')) {
      // "SCHEMA." => tables
      const schema = this.schemaLookup.get(parts[0].toUpperCase());
      if (schema) {
        for (const t of schema.tables ?? []) {
          suggestions.push({
            label: t.name,
            insertText: t.name,
            kind: monaco.languages.CompletionItemKind.Class,
            detail: `${t.columns?.length ?? 0} columns`,
            range
          });
        }
        return { suggestions: this.dedupeSuggestions(suggestions) };
      }
    }

    if (parts.length === 2 && token.endsWith('.')) {
      // "ALIAS." or "TABLE." => columns
      const base = parts[0];
      const ref = tableMap[base] || tableMap[base.toUpperCase()];
      if (ref) {
        this.suggestColumnsForTable(monaco, suggestions, range, ref.schema, ref.table, base);
        return { suggestions: this.dedupeSuggestions(suggestions) };
      }

      // maybe schema.table. (user typed "SCHEMA.TABLE.")
      const schemaUpper = parts[0].toUpperCase();
      const schema = this.schemaLookup.get(schemaUpper);
      if (schema) {
        const t = schema.tables?.find(x => x.name.toUpperCase() === parts[1].toUpperCase());
        if (t) {
          for (const c of t.columns ?? []) {
            suggestions.push({
              label: `${parts[0]}.${parts[1]}.${c}`,
              insertText: c,
              kind: monaco.languages.CompletionItemKind.Field,
              detail: `${parts[0]}.${parts[1]}.${c}`,
              range
            });
          }
          return { suggestions: this.dedupeSuggestions(suggestions) };
        }
      }
    }

    if (parts.length >= 3) {
      // "SCHEMA.TABLE.COLPREFIX" => columns
      const schemaName = parts[0];
      const tableName = parts[1];
      this.suggestColumnsForTable(monaco, suggestions, range, schemaName, tableName, `${schemaName}.${tableName}`);
      return { suggestions: this.dedupeSuggestions(suggestions) };
    }

    // 4) Advanced: INSERT column list context
    if (ctx === 'INSERT_COLS') {
      // Suggest columns; optionally auto-close ')'
      if (insertTarget) {
        this.suggestInsertColumns(monaco, suggestions, range, insertTarget, {
          autoCloseParen: true,
          addColumnsAndValuesSnippet: true
        });
      } else {
        // fallback: still provide generic
        this.suggestKeywords(monaco, suggestions, range, config.sqlKeywords);
      }

      return { suggestions: this.dedupeSuggestions(suggestions) };
    }

    // 5) Context-aware suggestions
    switch (ctx) {
      case 'UPDATE': {
        this.suggestTables(monaco, suggestions, range, config);
        this.suggestKeywords(monaco, suggestions, range, ['UPDATE', 'SET', 'WHERE']);
        break;
      }

      case 'SET': {
        // 5A) UPDATE ... SET snippet suggestions
        if (updateTarget) {
          this.suggestUpdateSetSnippets(monaco, suggestions, range, updateTarget);
          // Also normal column completion (so typing "col" works)
          this.suggestColumnsForTable(monaco, suggestions, range, updateTarget.schema, updateTarget.table, updateTarget.table);
        } else if (Object.keys(tableMap).length) {
          this.suggestColumnsFromActiveTables(monaco, suggestions, range, tableMap);
        }
        this.suggestOperators(monaco, suggestions, range, config.sqlOperators);
        this.suggestKeywords(monaco, suggestions, range, ['WHERE']);
        break;
      }

      case 'DELETE': {
        this.suggestKeywords(monaco, suggestions, range, ['FROM']);
        this.suggestKeywords(monaco, suggestions, range, config.sqlKeywords);
        break;
      }

      case 'FROM':
      case 'JOIN':
      case 'INTO': {
        this.suggestSchemas(monaco, suggestions, range, config);
        this.suggestTables(monaco, suggestions, range, config);
        break;
      }

      case 'WHERE':
      case 'HAVING':
      case 'ON': {
        this.suggestColumnsFromActiveTables(monaco, suggestions, range, tableMap);
        this.suggestOperators(monaco, suggestions, range, config.sqlOperators);
        this.suggestKeywords(monaco, suggestions, range, [
          'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'LIKE', 'BETWEEN', 'IS', 'NULL'
        ]);
        break;
      }

      case 'SELECT':
      case 'ORDER':
      case 'GROUP': {
        if (Object.keys(tableMap).length) {
          this.suggestColumnsFromActiveTables(monaco, suggestions, range, tableMap);
        } else {
          this.suggestSchemas(monaco, suggestions, range, config);
          this.suggestTables(monaco, suggestions, range, config);
        }
        this.suggestKeywords(monaco, suggestions, range, config.sqlKeywords);
        this.suggestBuiltins(monaco, suggestions, range, config);
        break;
      }

      default: {
        if (Object.keys(tableMap).length) {
          this.suggestColumnsFromActiveTables(monaco, suggestions, range, tableMap);
        } else {
          this.suggestSchemas(monaco, suggestions, range, config);
          this.suggestTables(monaco, suggestions, range, config);
        }
        this.suggestKeywords(monaco, suggestions, range, config.sqlKeywords);
        this.suggestBuiltins(monaco, suggestions, range, config);
        break;
      }
    }

    this.filterByTypedPrefix(model, position, suggestions);
    return { suggestions: this.dedupeSuggestions(suggestions) };
  }

  // -------------------------
  // Variable IntelliSense
  // -------------------------
  private detectVariableContext(textUntil: string): { inVar: boolean; scope?: string; prefix?: string } {
    const i = textUntil.lastIndexOf('${');
    if (i < 0) return { inVar: false };

    const after = textUntil.substring(i + 2);
    if (after.includes('}')) return { inVar: false };

    const inside = after;
    const parts = inside.split('.');
    if (parts.length === 1) return { inVar: true, prefix: parts[0] ?? '' };
    return { inVar: true, scope: (parts[0] ?? '').trim(), prefix: (parts[1] ?? '').trim() };
  }

  private addWorkflowVarSuggestions(monaco: any, suggestions: any[], range: any, varCtx: { scope?: string; prefix?: string }): void {
    const prefix = (varCtx.prefix ?? '').toUpperCase();

    if (!varCtx.scope) {
      const scopes = Array.from(this.variableScopes.keys()).sort();
      for (const s of scopes) {
        if (prefix && !s.toUpperCase().startsWith(prefix)) continue;
        suggestions.push({
          label: s,
          kind: monaco.languages.CompletionItemKind.Variable,
          insertText: s,
          detail: 'workflow scope',
          range
        });
      }
      return;
    }

    const keys = this.variableScopes.get(varCtx.scope) ?? [];
    for (const k of keys) {
      if (prefix && !k.toUpperCase().startsWith(prefix)) continue;
      suggestions.push({
        label: k,
        kind: monaco.languages.CompletionItemKind.Variable,
        insertText: k,
        detail: `${varCtx.scope}.${k}`,
        range
      });
    }
  }

  // -------------------------
  // SQL Context / Scope
  // -------------------------
  private stripStrings(sql: string): string {
    return sql
      .replace(/'([^']|'')*'/g, "''")
      .replace(/"([^"]|"")*"/g, '""');
  }

  private currentStatement(textUntilCursor: string): string {
    const parts = textUntilCursor.split(';');
    return parts[parts.length - 1] ?? textUntilCursor;
  }

  private detectContext(textUntilCursor: string, stmtStripped: string): SqlCtx {
    const tail = stmtStripped.replace(/\s+/g, ' ').trim().toUpperCase();

    // INSERT column list: INSERT INTO table ( <cursor>
    // If cursor is after "(" belonging to INSERT INTO ... and not closed yet in this scope
    if (/\bINSERT\s+INTO\s+[\w.]+\s*\(\s*[^)]*$/.test(tail)) return 'INSERT_COLS';

    if (/\bSET\b[^;]*$/.test(tail) && /\bUPDATE\b/.test(tail)) return 'SET';
    if (/\bWHERE\b[^;]*$/.test(tail)) return 'WHERE';
    if (/\bHAVING\b[^;]*$/.test(tail)) return 'HAVING';
    if (/\bORDER BY\b[^;]*$/.test(tail)) return 'ORDER';
    if (/\bGROUP BY\b[^;]*$/.test(tail)) return 'GROUP';

    if (/\bON\b[^;]*$/.test(tail) && /\bJOIN\b/.test(tail)) return 'ON';
    if (/\bJOIN\b[^;]*$/.test(tail)) return 'JOIN';
    if (/\bFROM\b[^;]*$/.test(tail)) return 'FROM';

    if (/\bUPDATE\b[^;]*$/.test(tail)) return 'UPDATE';
    if (/\bDELETE\b[^;]*$/.test(tail)) return 'DELETE';
    if (/\bINSERT\b[^;]*$/.test(tail)) return 'INSERT';
    if (/\bINTO\b[^;]*$/.test(tail)) return 'INTO';
    if (/\bVALUES\b[^;]*$/.test(tail)) return 'VALUES';

    if (/\bSELECT\b[^;]*$/.test(tail)) return 'SELECT';
    return 'NONE';
  }

  /**
   * Depth-aware: returns the substring that is within the same parentheses depth as the cursor.
   * This makes alias/table detection much more accurate for nested subqueries.
   */
  private extractScopeAtCursor(stmt: string): string {
    let depth = 0;
    for (let i = 0; i < stmt.length; i++) {
      const ch = stmt[i];
      if (ch === '(') depth++;
      else if (ch === ')') depth = Math.max(0, depth - 1);
    }
    const cursorDepth = depth;

    // Walk backwards to find where this depth segment starts
    let start = stmt.length;
    depth = cursorDepth;
    for (let i = stmt.length - 1; i >= 0; i--) {
      const ch = stmt[i];
      if (ch === ')') depth++;
      else if (ch === '(') depth = Math.max(0, depth - 1);

      if (depth < cursorDepth) {
        start = i + 1;
        break;
      }
    }

    // Walk forward (not strictly necessary) - keep near-tail scope
    return stmt.substring(start);
  }

  private parseTableRefsDepthAware(scopeUpper: string): {
    tableMap: Record<string, TableRef>;
    updateTarget?: TableRef;
    insertTarget?: TableRef;
  } {
    const tableMap: Record<string, TableRef> = {};

    // Only match FROM/JOIN at this scope (scopeUpper already extracted)
    const tableRegex = /\b(FROM|JOIN)\s+([\w.]+)(?:\s+(?:AS\s+)?(\w+))?/gi;
    let m: RegExpExecArray | null;
    while ((m = tableRegex.exec(scopeUpper)) !== null) {
      const raw = m[2];
      const alias = m[3];
      const parts = raw.split('.');
      const schema = parts.length === 2 ? parts[0] : undefined;
      const table = parts.length === 2 ? parts[1] : parts[0];

      if (alias) tableMap[alias] = { schema, table };
      tableMap[table] = { schema, table };
    }

    // UPDATE target in this scope
    const upd = /\bUPDATE\s+([\w.]+)/i.exec(scopeUpper);
    const updateTarget = upd
      ? (() => {
          const parts = upd[1].split('.');
          return {
            schema: parts.length === 2 ? parts[0] : undefined,
            table: parts.length === 2 ? parts[1] : parts[0]
          };
        })()
      : undefined;

    // INSERT target in this scope
    const ins = /\bINSERT\s+INTO\s+([\w.]+)/i.exec(scopeUpper);
    const insertTarget = ins
      ? (() => {
          const parts = ins[1].split('.');
          return {
            schema: parts.length === 2 ? parts[0] : undefined,
            table: parts.length === 2 ? parts[1] : parts[0]
          };
        })()
      : undefined;

    // DELETE FROM target
    const del = /\bDELETE\s+FROM\s+([\w.]+)/i.exec(scopeUpper);
    if (del) {
      const parts = del[1].split('.');
      const schema = parts.length === 2 ? parts[0] : undefined;
      const table = parts.length === 2 ? parts[1] : parts[0];
      tableMap[table] = { schema, table };
    }

    return { tableMap, updateTarget, insertTarget };
  }

  private lastToken(textUntil: string): string {
    const m = /([\w$.]+)$/.exec(textUntil);
    return m?.[1] ?? '';
  }

  // -------------------------
  // Suggestions
  // -------------------------
  private suggestSchemas(monaco: any, suggestions: any[], range: any, config: IntellisenseConfig): void {
    for (const s of config.metadata ?? []) {
      suggestions.push({
        label: s.name,
        insertText: s.name,
        kind: monaco.languages.CompletionItemKind.Module,
        detail: `${s.tables?.length ?? 0} tables`,
        range
      });
    }
  }

  private suggestTables(monaco: any, suggestions: any[], range: any, config: IntellisenseConfig): void {
    for (const s of config.metadata ?? []) {
      for (const t of s.tables ?? []) {
        suggestions.push({
          label: `${s.name}.${t.name}`,
          insertText: `${s.name}.${t.name}`,
          kind: monaco.languages.CompletionItemKind.Class,
          detail: `${t.columns?.length ?? 0} columns`,
          range
        });
      }
    }
  }

  private suggestColumnsForTable(monaco: any, suggestions: any[], range: any, schema: string | undefined, table: string, labelPrefix: string): void {
    const key = (schema ? `${schema}.${table}` : table).toUpperCase();
    const info = this.tableLookup.get(key);
    if (!info) return;

    for (const c of info.table.columns ?? []) {
      suggestions.push({
        label: `${labelPrefix}.${c}`,
        insertText: c,
        kind: monaco.languages.CompletionItemKind.Field,
        detail: schema ? `${schema}.${table}.${c}` : `${table}.${c}`,
        range
      });
    }
  }

  private suggestColumnsFromActiveTables(monaco: any, suggestions: any[], range: any, tableMap: Record<string, TableRef>): void {
    const seen = new Set<string>();

    for (const aliasOrTable of Object.keys(tableMap)) {
      const ref = tableMap[aliasOrTable];
      const key = (ref.schema ? `${ref.schema}.${ref.table}` : ref.table).toUpperCase();
      const info = this.tableLookup.get(key);
      if (!info) continue;

      for (const c of info.table.columns ?? []) {
        const dotted = `${aliasOrTable}.${c}`;
        suggestions.push({
          label: dotted,
          insertText: dotted,
          kind: monaco.languages.CompletionItemKind.Field,
          detail: `${ref.schema ? ref.schema + '.' : ''}${ref.table}.${c}`,
          range
        });

        if (!seen.has(c)) {
          seen.add(c);
          suggestions.push({
            label: c,
            insertText: c,
            kind: monaco.languages.CompletionItemKind.Field,
            detail: dotted,
            range
          });
        }
      }
    }
  }

  private suggestBuiltins(monaco: any, suggestions: any[], range: any, config: IntellisenseConfig): void {
    const list = config.dbBuiltins?.[config.dbType] ?? [];
    for (const fn of list) {
      suggestions.push({
        label: fn.name,
        kind: monaco.languages.CompletionItemKind.Function,
        insertText: `${fn.name}()`,
        detail: fn.description,
        range
      });
    }
  }

  private suggestKeywords(monaco: any, suggestions: any[], range: any, words: string[]): void {
    for (const k of words) {
      suggestions.push({
        label: k,
        insertText: k,
        kind: monaco.languages.CompletionItemKind.Keyword,
        range
      });
    }
  }

  private suggestOperators(monaco: any, suggestions: any[], range: any, ops: string[]): void {
    for (const op of ops) {
      suggestions.push({
        label: op,
        insertText: op,
        kind: monaco.languages.CompletionItemKind.Operator,
        range
      });
    }
  }

  // -------------------------
  // UPDATE ... SET snippets
  // -------------------------
  private suggestUpdateSetSnippets(monaco: any, suggestions: any[], range: any, target: TableRef): void {
    const cols = this.getColumnsFor(target);
    if (!cols.length) return;

    // (A) One big "SET all columns" snippet (limit to keep sane)
    const maxCols = 25;
    const useCols = cols.slice(0, maxCols);

    const lines = useCols.map((c, i) => `${c} = \${${i + 1}:value}`);
    const bigSnippet = lines.join(',\n  ') + (cols.length > maxCols ? ',\n  -- ...' : '');

    suggestions.push({
      label: 'SET all columns (snippet)',
      kind: monaco.languages.CompletionItemKind.Snippet,
      insertText: `  ${bigSnippet}`,
      insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
      detail: `${this.fq(target)} – generates assignments`,
      range
    });

    // (B) Per-column assignment snippet (best day-to-day)
    for (const c of cols) {
      suggestions.push({
        label: `${c} = (snippet)`,
        kind: monaco.languages.CompletionItemKind.Snippet,
        insertText: `${c} = \${1:value}`,
        insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
        detail: `Assign ${c}`,
        range
      });
    }
  }

  // -------------------------
  // INSERT INTO table ( ... ) support
  // -------------------------
  private suggestInsertColumns(
    monaco: any,
    suggestions: any[],
    range: any,
    target: TableRef,
    opts: { autoCloseParen: boolean; addColumnsAndValuesSnippet: boolean }
  ): void {
    const cols = this.getColumnsFor(target);
    if (!cols.length) return;

    // (A) Column-by-column completion (nice when typing)
    for (const c of cols) {
      suggestions.push({
        label: c,
        kind: monaco.languages.CompletionItemKind.Field,
        insertText: c,
        detail: `${this.fq(target)}.${c}`,
        range
      });
    }

    // (B) Insert a list of columns and auto-close ')'
    const maxCols = 25;
    const useCols = cols.slice(0, maxCols);
    const colList = useCols.join(', ') + (cols.length > maxCols ? ', /*...*/' : '');
    const close = opts.autoCloseParen ? ')' : '';

    suggestions.push({
      label: 'Columns list (auto-close )',
      kind: monaco.languages.CompletionItemKind.Snippet,
      insertText: `${colList}${close}`,
      insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
      detail: `${this.fq(target)} – column list`,
      range
    });

    // (C) Columns + VALUES snippet
    if (opts.addColumnsAndValuesSnippet) {
      const values = useCols.map((_, i) => `\${${i + 1}:value}`).join(', ');
      suggestions.push({
        label: 'Columns + VALUES (snippet)',
        kind: monaco.languages.CompletionItemKind.Snippet,
        insertText: `${colList}) VALUES (${values})`,
        insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
        detail: 'Generates VALUES placeholders',
        range
      });
    }
  }

  private getColumnsFor(target: TableRef): string[] {
    const key = (target.schema ? `${target.schema}.${target.table}` : target.table).toUpperCase();
    const info = this.tableLookup.get(key);
    return info?.table?.columns ?? [];
  }

  private fq(target: TableRef): string {
    return target.schema ? `${target.schema}.${target.table}` : target.table;
  }

  // -------------------------
  // Polish
  // -------------------------
  private filterByTypedPrefix(model: any, position: any, suggestions: any[]): void {
    const w = model.getWordUntilPosition(position);
    const typed = (w?.word ?? '').toUpperCase();
    if (!typed) return;

    for (let i = suggestions.length - 1; i >= 0; i--) {
      const label = typeof suggestions[i].label === 'string' ? suggestions[i].label : suggestions[i].label?.label;
      if (!label) continue;

      const L = String(label).toUpperCase();
      if (L.includes('.')) continue;
      if (!L.startsWith(typed)) suggestions.splice(i, 1);
    }
  }

  private dedupeSuggestions(suggestions: any[]): any[] {
    const unique = new Map<string, any>();
    for (const s of suggestions) {
      const key = typeof s.label === 'string' ? s.label : s.label?.label;
      if (!key) continue;
      unique.set(String(key), s);
    }
    return Array.from(unique.values());
  }
}
