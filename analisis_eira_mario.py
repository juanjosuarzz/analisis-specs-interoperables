import pandas as pd
import numpy as np

# Optional visualization
try:
    import plotly.express as px
except Exception:  # pragma: no cover
    px = None


INPUT_FILE_DEFAULT = 'elis-pre-eira-7 (modificadoMario).xlsx'
OUTPUT_FILE_DEFAULT = 'Analisis_EIRA_Mario_Final.xlsx'

def _normalize_col(df: pd.DataFrame, col: str) -> str:
    """Return actual column name in df that matches `col` ignoring case and whitespace."""
    target = ''.join(col.lower().split())
    for c in df.columns:
        if ''.join(str(c).lower().split()) == target:
            return c
    raise KeyError(f"Column '{col}' not found. Available: {list(df.columns)}")

def load_workbook(file_path: str):
    """Load required sheets from the Excel workbook."""
    xls = pd.ExcelFile(file_path)
    sheets = {s: pd.read_excel(xls, sheet_name=s) for s in xls.sheet_names}
    return sheets

def pick_main_sheet(sheets: dict) -> str:
    """Prefer 'scope' if present, else 'Sheet1' if present, else first sheet."""
    if 'scope' in sheets:
        return 'scope'
    if 'Sheet1' in sheets:
        return 'Sheet1'
    # fallback
    return next(iter(sheets.keys()))

def build_hierarchy(herencias_df: pd.DataFrame) -> pd.DataFrame:
    """Prepare hierarchy table with normalized parent/child columns."""
    parent_col = _normalize_col(herencias_df, 'Parent ABB')
    child_col = _normalize_col(herencias_df, 'Child ABB')

    h = herencias_df[[parent_col, child_col]].copy()
    h.columns = ['Parent ABB', 'Child ABB']
    h['Parent ABB'] = h['Parent ABB'].astype(str).str.strip()
    h['Child ABB'] = h['Child ABB'].astype(str).str.strip()

    # Drop blanks
    h = h[(h['Child ABB'] != '') & (h['Child ABB'].str.lower() != 'nan')]
    h = h.drop_duplicates().reset_index(drop=True)
    return h

def compute_levels(h: pd.DataFrame):
    """Compute level (depth) from roots for each ABB and the root parent."""
    parents = set(h['Parent ABB'])
    children = set(h['Child ABB'])
    roots = sorted(list(parents - children))

    # adjacency from parent to children
    adj = {}
    for p, c in h[['Parent ABB', 'Child ABB']].itertuples(index=False):
        adj.setdefault(p, []).append(c)

    level = {}
    root_of = {}

    from collections import deque

    q = deque()
    for r in roots:
        level[r] = 0
        root_of[r] = r
        q.append(r)

    while q:
        node = q.popleft()
        for ch in adj.get(node, []):
            if ch not in level:
                level[ch] = level[node] + 1
                root_of[ch] = root_of[node]
                q.append(ch)

    # Handle disconnected nodes (cycles or missing roots)
    for n in sorted(list(parents | children)):
        if n not in level:
            level[n] = np.nan
            root_of[n] = None

    levels_df = pd.DataFrame({
        'ABB': list(level.keys()),
        'EIRA Level': list(level.values()),
        'Root Parent ABB': [root_of[k] for k in level.keys()],
    })
    return roots, adj, levels_df

def attach_hierarchy_to_specs(main_df: pd.DataFrame, h_levels: pd.DataFrame) -> pd.DataFrame:
    """Attach EIRA hierarchy metadata to each spec row based on its assigned ABB."""
    abb_col = None
    for candidate in ['ABB', 'Child ABB', 'ABBs', 'EIRA ABB']:
        try:
            abb_col = _normalize_col(main_df, candidate)
            break
        except KeyError:
            continue
    if abb_col is None:
        raise KeyError("Could not find an ABB column in main sheet. Expected one of: ABB, Child ABB, ABBs, EIRA ABB")

    view_col = None
    for candidate in ['View', 'EIRA View']:
        try:
            view_col = _normalize_col(main_df, candidate)
            break
        except KeyError:
            continue

    specs = main_df.copy()
    specs['ABB'] = specs[abb_col].astype(str).str.strip()
    if view_col is not None:
        specs['View'] = specs[view_col].astype(str).str.strip()

    # Merge level/root info
    m = h_levels.rename(columns={'ABB': 'ABB'})
    specs = specs.merge(m, how='left', on='ABB')

    # Derive immediate parent if ABB is a child in herencias
    # This is useful for coverage by Parent ABB.
    return specs

def compute_parent_lookup(h: pd.DataFrame) -> dict:
    """Map child -> list(parents). Usually 1, but keep list for safety."""
    from collections import defaultdict
    d = defaultdict(list)
    for p, c in h[['Parent ABB', 'Child ABB']].itertuples(index=False):
        d[c].append(p)
    return dict(d)

def add_parent_columns(specs: pd.DataFrame, parent_lookup: dict) -> pd.DataFrame:
    specs = specs.copy()
    specs['Parent ABB(s)'] = specs['ABB'].map(lambda x: parent_lookup.get(x, []))
    specs['Parent ABB'] = specs['Parent ABB(s)'].map(lambda xs: xs[0] if isinstance(xs, list) and len(xs) else None)
    return specs

def coverage_pivot(specs: pd.DataFrame) -> pd.DataFrame:
    if 'View' not in specs.columns:
        raise KeyError("No 'View' column found in main data; cannot compute View x Parent ABB pivot.")
    piv = pd.pivot_table(
        specs,
        index=['View', 'Parent ABB'],
        values='ABB',
        aggfunc='count',
        fill_value=0,
    ).rename(columns={'ABB': 'Specifications Count'}).reset_index()
    return piv

def pareto_parent_abbs(specs: pd.DataFrame, threshold: float = 0.8) -> pd.DataFrame:
    counts = specs['Parent ABB'].fillna('<<NO PARENT>>').value_counts().rename_axis('Parent ABB').reset_index(name='Count')
    total = counts['Count'].sum()
    counts['CumCount'] = counts['Count'].cumsum()
    counts['CumPct'] = counts['CumCount'] / total if total else 0
    critical = counts[counts['CumPct'] <= threshold].head(5)
    # Ensure at least 5 rows if possible
    if len(critical) < 5:
        critical = counts.head(5)
    return critical

def gaps_child_abbs(h: pd.DataFrame, specs: pd.DataFrame) -> pd.DataFrame:
    all_children = set(h['Child ABB'])
    used = set(specs['ABB'].dropna().astype(str))
    missing = sorted(list(all_children - used))
    return pd.DataFrame({'Child ABB with no specification': missing})

def quality_by_level(specs: pd.DataFrame) -> pd.DataFrame:
    # Try to find quality columns
    score_col = None
    dist_col = None
    for c in ['Automated score(s)', 'Automated scores', 'Automated score']:
        try:
            score_col = _normalize_col(specs, c)
            break
        except KeyError:
            continue
    for c in ['Assessment distribution(s)', 'Assessment distribution']:
        try:
            dist_col = _normalize_col(specs, c)
            break
        except KeyError:
            continue

    q = specs.copy()
    if score_col is not None:
        q['_score_raw'] = q[score_col]
        q['Automated score(s) num'] = pd.to_numeric(q['_score_raw'], errors='coerce')
    else:
        q['Automated score(s) num'] = np.nan

    # Summaries by level
    grp = q.groupby('EIRA Level', dropna=False).agg(
        specs_count=('ABB', 'count'),
        avg_score=('Automated score(s) num', 'mean'),
        median_score=('Automated score(s) num', 'median'),
    ).reset_index()

    # Keep distribution as a sample list per level if exists
    if dist_col is not None:
        dist = q.groupby('EIRA Level', dropna=False)[dist_col].apply(lambda s: list(s.dropna().astype(str).unique())[:10]).reset_index(name='Assessment distribution(s) sample')
        grp = grp.merge(dist, on='EIRA Level', how='left')

    return grp

def create_sunburst(specs: pd.DataFrame, out_html: str = 'sunburst_eira.html'):
    if px is None:
        return None
    if 'View' not in specs.columns:
        return None
    sb = specs.copy()
    sb['Parent ABB'] = sb['Parent ABB'].fillna('<<NO PARENT>>')
    fig = px.sunburst(sb, path=['View', 'Parent ABB'], values=None)
    fig.update_traces(textinfo='label+percent parent')
    fig.write_html(out_html)
    return out_html

def print_summary(specs: pd.DataFrame, cov: pd.DataFrame, pareto: pd.DataFrame, gaps: pd.DataFrame, qual: pd.DataFrame):
    print('===== RESUMEN ESTADISTICO EIRA =====')
    print(f"Total filas (especificaciones): {len(specs)}")
    if 'View' in specs.columns:
        print('\n-- Especificaciones por View --')
        print(specs['View'].value_counts(dropna=False).to_string())

    print('\n-- Top Parent ABBs (Pareto) --')
    print(pareto.to_string(index=False))

    print('\n-- Gaps: Child ABBs sin especificaciones --')
    print(f"Total gaps: {len(gaps)}")

    print('\n-- Calidad por nivel (EIRA Level) --')
    print(qual.to_string(index=False))

    print('\n-- Cobertura (View x Parent ABB) [primeras 30 filas] --')
    print(cov.head(30).to_string(index=False))

def export_excel(specs: pd.DataFrame, cov: pd.DataFrame, pareto: pd.DataFrame, gaps: pd.DataFrame, qual: pd.DataFrame, out_path: str):
    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        specs.to_excel(writer, sheet_name='Specs_con_Herencia', index=False)
        cov.to_excel(writer, sheet_name='Cobertura_View_Parent', index=False)
        pareto.to_excel(writer, sheet_name='Pareto_ParentABB', index=False)
        gaps.to_excel(writer, sheet_name='Gaps_ChildABB', index=False)
        qual.to_excel(writer, sheet_name='Calidad_por_Nivel', index=False)

def main(input_file: str = INPUT_FILE_DEFAULT, output_file: str = OUTPUT_FILE_DEFAULT):
    sheets = load_workbook(input_file)

    # Hierarchy sheet
    if 'Herencias ABBs EIRA 7.0' not in sheets:
        raise KeyError(f"Missing required sheet 'Herencias ABBs EIRA 7.0'. Found: {list(sheets.keys())}")
    herencias = sheets['Herencias ABBs EIRA 7.0']
    h = build_hierarchy(herencias)
    roots, adj, h_levels = compute_levels(h)

    # Main sheet (specs)
    main_sheet = pick_main_sheet(sheets)
    main_df = sheets[main_sheet]

    specs = attach_hierarchy_to_specs(main_df, h_levels)
    parent_lookup = compute_parent_lookup(h)
    specs = add_parent_columns(specs, parent_lookup)

    cov = coverage_pivot(specs)
    pareto = pareto_parent_abbs(specs, threshold=0.8)
    gaps = gaps_child_abbs(h, specs)
    qual = quality_by_level(specs)

    print_summary(specs, cov, pareto, gaps, qual)
    export_excel(specs, cov, pareto, gaps, qual, output_file)

    # Optional visualization
    create_sunburst(specs)

if __name__ == '__main__':
    main()