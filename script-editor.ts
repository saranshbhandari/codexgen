
import {
  AfterViewInit,
  Component,
  ElementRef,
  Input,
  OnChanges,
  OnDestroy,
  SimpleChanges,
  ViewChild
} from '@angular/core';

import { SQL_KEYWORDS, SQL_OPERATORS } from './sql.constants';
import { SqlIntellisenseUtil, SchemaMeta, WorkflowVarDTO } from './sql-intellisense.util';

// Your existing constants (keep your own imports if you already have them)
import { DB_LANGUAGES } from './db-languages';
import { DB_BUILTINS } from './db-builtins';

declare const monaco: any;

type AnalysisRow = {
  ruleId: string;
  suggestion: string;
  severity: 'low' | 'medium' | 'high';
  startLine: number;
  startCol: number;
  endLine: number;
  endCol: number;
};

@Component({
  selector: 'app-script-editor',
  templateUrl: './script-editor.component.html',
  styleUrls: ['./script-editor.component.scss']
})
export class ScriptEditorComponent implements AfterViewInit, OnDestroy, OnChanges {
  @ViewChild('editorContainer', { static: true }) editorContainer!: ElementRef<HTMLDivElement>;

  @Input() script = '';
  @Input() databaseType = 'oracle';

  /**
   * metadata structure must match:
   * [{ name: 'SCHEMA', tables: [{ name: 'TABLE', columns: ['C1','C2'] }] }]
   */
  @Input() metadata: SchemaMeta[] | null = null;

  /**
   * Workflow vars structure:
   * [{ scope: 'SCOPE', variableKey: 'KEY' }]
   */
  @Input() workflowVariables: WorkflowVarDTO[] = [];

  editor: any;
  provider?: any;
  hoverProvider?: any;
  semanticProvider?: any;

  theme: 'light' | 'dark' = 'light';
  languageId = 'sql';

  metadataAvailable = false;

  zoomSliderValue = 12;

  analysisVisible = false;
  analysis: { findings: AnalysisRow[] } = { findings: [] };

  isDragOver = false;

  private sqlUtil = new SqlIntellisenseUtil();

  private getLanguage(): string {
    return DB_LANGUAGES[this.databaseType] || 'sql';
  }

  ngAfterViewInit(): void {
    this.languageId = this.getLanguage();

    if (typeof monaco === 'undefined') return;

    this.defineThemes();
    this.registerCompletions();
    this.registerHovers();
    this.registerSemanticTokens();

    this.editor = monaco.editor.create(this.editorContainer.nativeElement, {
      value: this.script ?? '',
      language: this.languageId,
      theme: this.theme === 'light' ? 'sql-light' : 'sql-dark',
      automaticLayout: true,
      minimap: { enabled: false },
      fontSize: this.zoomSliderValue
    });

    this.editor.onDidChangeModelContent(() => {
      this.script = this.editor.getValue();
      this.markDDL();
    });

    // hotkey for analysis (cmd/ctrl + ~)
    window.addEventListener('keydown', this.onKeyDown);
    this.markDDL();

    this.refreshLookups();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['databaseType'] && !changes['databaseType'].firstChange) {
      this.languageId = this.getLanguage();
      this.refreshLookups();
      // editor language update
      if (this.editor?.getModel()) monaco.editor.setModelLanguage(this.editor.getModel(), this.languageId);
    }

    if (changes['metadata'] || changes['workflowVariables']) {
      this.refreshLookups();
    }

    if (changes['script'] && this.editor && this.editor.getValue() !== (this.script ?? '')) {
      this.editor.setValue(this.script ?? '');
      this.markDDL();
    }
  }

  ngOnDestroy(): void {
    window.removeEventListener('keydown', this.onKeyDown);
    this.provider?.dispose?.();
    this.hoverProvider?.dispose?.();
    this.semanticProvider?.dispose?.();
    this.editor?.dispose?.();
  }

  // ---------------- UI actions ----------------

  toggleTheme(): void {
    this.theme = this.theme === 'light' ? 'dark' : 'light';
    if (typeof monaco !== 'undefined') {
      monaco.editor.setTheme(this.theme === 'light' ? 'sql-light' : 'sql-dark');
    }
  }

  prettify(): void {
    const text = this.editor ? this.editor.getValue() : this.script;
    const pretty = this.formatSQL(text);
    if (this.editor) this.editor.setValue(pretty);
    else this.script = pretty;
  }

  getBeautifyScript(): string {
    const text = this.editor ? this.editor.getValue() : this.script;
    return this.formatSQL(text);
  }

  getMinimizedScript(): string {
    const text = this.editor ? this.editor.getValue() : this.script;
    return text.replace(/\s+/g, ' ').trim();
  }

  download(): void {
    const text = this.editor ? this.editor.getValue() : this.script;
    const blob = new Blob([text], { type: 'text/sql' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'script.sql';
    a.click();
    URL.revokeObjectURL(url);
  }

  copy(): void {
    const text = this.getMinimizedScript();
    navigator.clipboard?.writeText(text);
  }

  copyFormatted(): void {
    const text = this.getBeautifyScript();
    navigator.clipboard?.writeText(text);
  }

  toggleAnalysis(): void {
    this.analysisVisible = !this.analysisVisible;
  }

  goTo(row: AnalysisRow): void {
    if (!this.editor) return;
    this.editor.setPosition({ lineNumber: row.startLine, column: row.startCol });
    this.editor.revealPositionInCenter({ lineNumber: row.startLine, column: row.startCol });
    this.editor.focus();
  }

  handleZoomSliderChange(v: number): void {
    this.zoomSliderValue = v;
    if (this.editor) this.editor.updateOptions({ fontSize: this.zoomSliderValue });
  }

  // ---------------- Drag/drop ----------------

  onDragEnter(event: DragEvent): void {
    event.preventDefault();
    this.isDragOver = true;
  }

  onDragOver(event: DragEvent): void {
    event.preventDefault();
  }

  onDragLeave(event: DragEvent): void {
    event.preventDefault();
    const related = (event as any).relatedTarget as Node | null;
    if (!related || !(event.currentTarget as Node).contains(related)) {
      this.isDragOver = false;
    }
  }

  onFileDrop(event: DragEvent): void {
    event.preventDefault();
    this.isDragOver = false;

    const files = event.dataTransfer?.files;
    if (!files || files.length === 0) return;

    const file = files[0];
    if (!file.name.endsWith('.sql') && !file.name.endsWith('.txt')) return;

    const reader = new FileReader();
    reader.onload = (e: any) => {
      const content = e.target.result as string;
      if (this.editor) this.editor.setValue(content);
      else this.script = content;
    };
    reader.readAsText(file);
  }

  // ---------------- Monaco registrations ----------------

  private defineThemes(): void {
    monaco.editor.defineTheme('sql-dark', {
      base: 'vs-dark',
      inherit: true,
      rules: [
        { token: 'schema', foreground: 'C586C0' },
        { token: 'table', foreground: '4EC9B0' },
        { token: 'column', foreground: '9CDCFE' }
      ],
      colors: {},
      semanticHighlighting: true
    } as any);

    monaco.editor.defineTheme('sql-light', {
      base: 'vs',
      inherit: true,
      rules: [
        { token: 'schema', foreground: '800000' },
        { token: 'table', foreground: '267f99' },
        { token: 'column', foreground: '795E26' }
      ],
      colors: {},
      semanticHighlighting: true
    } as any);
  }

  private registerCompletions(): void {
    this.provider?.dispose?.();

    this.provider = monaco.languages.registerCompletionItemProvider(this.languageId, {
      triggerCharacters: [' ', '.', '(', ')', ',', '$', '{', '}', '=', '>', '<'],
      provideCompletionItems: (model: any, position: any) => {
        const cfg = this.buildConfig();
        return this.sqlUtil.buildCompletions({
          monaco,
          model,
          position,
          languageId: this.languageId,
          config: cfg
        });
      }
    });
  }

  private registerHovers(): void {
    this.hoverProvider?.dispose?.();

    this.hoverProvider = monaco.languages.registerHoverProvider(this.languageId, {
      provideHover: (model: any, position: any) => {
        const word = model.getWordAtPosition(position);
        if (!word) return { contents: [] };

        const text = word.word;
        const builtins = DB_BUILTINS[this.databaseType] || [];
        const fn = builtins.find((b: any) => b.name === text.toUpperCase());

        if (fn) {
          return {
            contents: [
              { value: `**${fn.name}**` },
              { value: fn.description },
              { value: `Example: \`${fn.sample}\`` }
            ]
          };
        }

        const schema = (this.metadata ?? []).find(s => s.name === text);
        if (schema) {
          return {
            contents: [{ value: `**${schema.name}** - ${schema.tables?.length ?? 0} tables` }]
          };
        }

        return { contents: [] };
      }
    });
  }

  private registerSemanticTokens(): void {
    this.semanticProvider?.dispose?.();

    const legend: any = {
      tokenTypes: ['schema', 'table', 'column'],
      tokenModifiers: []
    };

    this.semanticProvider = monaco.languages.registerDocumentSemanticTokensProvider(this.languageId, {
      getLegend: () => legend,
      provideDocumentSemanticTokens: (model: any) => {
        const lines = model.getLinesContent();
        const data: number[] = [];
        let lastLine = 0;
        let lastChar = 0;

        const push = (line: number, start: number, length: number, type: number) => {
          const deltaLine = line - lastLine;
          const deltaStart = deltaLine === 0 ? start - lastChar : start;
          data.push(deltaLine, deltaStart, length, type, 0);
          lastLine = line;
          lastChar = start;
        };

        (this.metadata ?? []).forEach(s => {
          const sReg = new RegExp(`\\b${this.escapeReg(s.name)}\\b`, 'g');
          lines.forEach((line, idx) => {
            let m: RegExpExecArray | null;
            while ((m = sReg.exec(line)) !== null) push(idx, m.index, s.name.length, 0);
          });

          (s.tables ?? []).forEach(t => {
            const tReg = new RegExp(`\\b${this.escapeReg(t.name)}\\b`, 'g');
            lines.forEach((line, idx) => {
              let m: RegExpExecArray | null;
              while ((m = tReg.exec(line)) !== null) push(idx, m.index, t.name.length, 1);
            });

            (t.columns ?? []).forEach(c => {
              const cReg = new RegExp(`\\b${this.escapeReg(c)}\\b`, 'g');
              lines.forEach((line, idx) => {
                let m: RegExpExecArray | null;
                while ((m = cReg.exec(line)) !== null) push(idx, m.index, c.length, 2);
              });
            });
          });
        });

        return { data: new Uint32Array(data) };
      },
      releaseDocumentSemanticTokens: () => {}
    });
  }

  // ---------------- helpers ----------------

  private refreshLookups(): void {
    const cfg = this.buildConfig();
    this.sqlUtil.rebuildLookups(cfg);
    this.metadataAvailable = !!(this.metadata && this.metadata.length > 0);
  }

  private buildConfig() {
    return {
      dbType: this.databaseType,
      metadata: this.metadata ?? [],
      dbBuiltins: DB_BUILTINS,
      sqlKeywords: SQL_KEYWORDS,
      sqlOperators: SQL_OPERATORS,
      workflowVars: (this.workflowVariables ?? []).map(v => ({
        scope: v.scope,
        variableKey: v.variableKey
      }))
    };
  }

  private escapeReg(s: string): string {
    return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  private markDDL(): void {
    if (typeof monaco === 'undefined' || !this.editor) return;

    const text = this.editor.getValue();
    const regex = /\b(CREATE|ALTER|DROP)\b/gi;
    const model = this.editor.getModel();
    const matches: any[] = [];
    const lines = text.split('\n');

    lines.forEach((line: string, idx: number) => {
      let match: RegExpExecArray | null;
      while ((match = regex.exec(line)) !== null) {
        matches.push({
          startLineNumber: idx + 1,
          startColumn: match.index + 1,
          endLineNumber: idx + 1,
          endColumn: match.index + match[0].length + 1,
          message: 'DDL not allowed',
          severity: monaco.MarkerSeverity.Warning
        });
      }
    });

    monaco.editor.setModelMarkers(model, 'ddl', matches);
  }

  private onKeyDown = (e: KeyboardEvent) => {
    const isMac = navigator.platform.toLowerCase().includes('mac');
    const ctrlOrCmd = isMac ? e.metaKey : e.ctrlKey;

    // cmd/ctrl + `
    if (ctrlOrCmd && e.key === '`') {
      e.preventDefault();
      this.toggleAnalysis();
    }
  };

  private formatSQL(text: string): string {
    const keywords = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'JOIN', 'AND', 'OR', 'ON', 'SET', 'VALUES'];
    let formatted = text.replace(/\s+/g, ' ');
    keywords.forEach(k => {
      const re = new RegExp(`\\b${k}\\b`, 'gi');
      formatted = formatted.replace(re, `\n${k}`);
    });

    let indent = 0;
    formatted = formatted
      .trim()
      .split('\n')
      .map(line => {
        const trimmed = line.trim();
        const upper = trimmed.toUpperCase();
        if (upper.startsWith('SELECT')) {
          indent = 1;
          return trimmed;
        }
        return '  '.repeat(indent) + trimmed;
      })
      .join('\n');

    return formatted;
  }
}
