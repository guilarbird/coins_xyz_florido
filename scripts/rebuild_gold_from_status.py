#!/usr/bin/env python3
import duckdb, pandas as pd, re
from pathlib import Path
DB="warehouse/coins_xyz.duckdb"
OUT="gold"; Path(OUT).mkdir(exist_ok=True)

VIEWS = [
  "silver__analysis_analysis_2__analysis_analysis_2",
  "silver__credit_card___analysis_credit_card___analysis_2__credit_card___analysis_credit_card___analysis_2",
]

def cells(df):
    for _,row in df.iterrows():
        vals=[str(v) for v in row.values if pd.notna(v) and str(v)!="None"]
        if not vals: 
            yield None
        else:
            # pega a célula mais “rica” em ';'
            s=max(vals, key=lambda x:x.count(";"))
            yield [p.strip() for p in s.split(";")]

def parse_brl(s):
    s=re.sub(r"[^0-9,.\-]","", str(s or ""))
    s=s.replace(".","").replace(",",".")
    try:
        return float(s) if s not in ("",".","-","--") else None
    except: return None

def try_blocks(lines):
    """encontra todos os blocos Status -> Date/Amount -> linhas dd-mm-yyyy"""
    blocks=[]
    n=len(lines)
    for i in range(n):
        row = lines[i] or []
        low = [t.lower() for t in row]
        if "status" in low and "completed" in low and "projected" in low:
            col_completed = low.index("completed")
            col_projected = low.index("projected")
            # header de data aparece logo abaixo
            start = None
            for j in range(i+1, min(i+8, n)):
                r2 = lines[j] or []
                if r2 and r2[0].lower().startswith("date"):
                    start = j+1
                    break
            if start is None: 
                continue
            # coleta dados até quebrar o padrão dd-mm-yyyy ou dd/mm/yyyy
            rx=re.compile(r"\d{1,2}[-/]\d{1,2}[-/]\d{4}$")
            rows=[]
            k=start
            while k<n and lines[k]:
                toks=lines[k]
                if not toks or not rx.match(toks[0]): break
                # normaliza para primeiro dia do mês
                d=toks[0].replace("/","-")
                dd=pd.to_datetime(d, dayfirst=True, errors="coerce")
                if dd is pd.NaT: break
                m = dd.replace(day=1).date()
                c = parse_brl(toks[col_completed]) if col_completed<len(toks) else None
                p = parse_brl(toks[col_projected]) if col_projected<len(toks) else None
                rows.append({"month":m,"completed":c or 0.0,"projected":p or 0.0})
                k+=1
            if rows:
                df=pd.DataFrame(rows)
                # score: meses em 2025 + soma completed (preferir o painel principal)
                score = (df["month"].map(lambda d:d.year==2025).sum(), df["completed"].sum())
                blocks.append((score, df))
    return blocks

def main():
    con=duckdb.connect(DB)
    all_lines=[]
    for v in VIEWS:
        try:
            df=con.execute(f"SELECT * FROM {v}").df()
        except: 
            continue
        all_lines.extend(list(cells(df)))

    blocks=try_blocks(all_lines)
    if not blocks:
        raise SystemExit("Nenhum bloco Status encontrado. Confirme se a planilha foi ingerida.")

    # escolhe o bloco com melhor score
    blocks.sort(key=lambda x: x[0], reverse=True)
    chosen = blocks[0][1].copy()
    chosen = chosen.groupby("month", as_index=False).sum()

    # escreve debug pra conferência
    dbg=chosen.copy(); dbg["block_id"]=0
    dbg.to_csv(f"{OUT}/debug_status_blocks.csv", index=False)

    # gold canonical
    canon=[]
    for _,r in chosen.iterrows():
        canon.append({"tx_date":r["month"],"month":r["month"],"amount":r["completed"],"status":"Completed","source":"analysis_status"})
        if r["projected"] and r["projected"]!=0:
            canon.append({"tx_date":r["month"],"month":r["month"],"amount":r["projected"],"status":"Projected","source":"analysis_status"})
    canon=pd.DataFrame(canon)
    canon.to_parquet(f"{OUT}/expenses_canon.parquet", index=False)

    # barras (month,status)
    bars=(canon.groupby(["month","status"], as_index=False)["amount"]
                .sum().rename(columns={"amount":"total"}))
    bars.to_parquet(f"{OUT}/expenses_per_month_status.parquet", index=False)
    bars.to_csv(f"{OUT}/expenses_per_month_status.csv", index=False)

    # acumulado 2025
    c2025=canon[canon["month"].map(lambda d:getattr(d,"year",0)==2025)].copy()
    if not c2025.empty:
        p = (c2025.pivot_table(index="month", columns="status", values="amount", aggfunc="sum")
                   .fillna(0).sort_index())
        p["cum_completed"]=p.get("Completed",0).cumsum()
        p["cum_total"]=(p.get("Completed",0)+p.get("Projected",0)).cumsum()
        out=p.reset_index()[["month","cum_completed","cum_total"]]
        out.to_parquet(f"{OUT}/expenses_cumulative_2025.parquet", index=False)
        out.to_csv(f"{OUT}/expenses_cumulative_2025.csv", index=False)

    print("[gold] rebuilt from best Status-block. Check gold/expenses_per_month_status.csv e gold/expenses_cumulative_2025.csv")
if __name__=="__main__":
    main()
