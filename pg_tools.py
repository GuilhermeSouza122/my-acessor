import os
from dotenv import load_dotenv
import psycopg2
from typing import Optional
from langchain.tools import tool
from pydantic import BaseModel, Field   

load_dotenv()


def get_conn():
    return psycopg2.connect(
    host = os.getenv("host"),
    database =os.getenv("database"),    
    user = os.getenv("user"),
    password = os.getenv("password"),
    port = os.getenv("port")
)

class AddTransactionArgs(BaseModel):
    amount: float = Field(..., description="Valor da transação (use positivo).")
    source_text: str = Field(..., description="Texto original do usuário.")
    occurred_at: Optional[str] = Field(
        default=None,
        description="Timestamp ISO 8601; se ausente, usa NOW() no banco."
    )
    type_id: Optional[int] = Field(default=None, description="ID em transaction_types (1=INCOME, 2=EXPENSES, 3=TRANSFER).")
    type_name: Optional[str] = Field(default=None, description="Nome do tipo: INCOME | EXPENSES | TRANSFER.")
    category_id: Optional[int] = Field(default=None, description="FK de categories (opcional).")
    description: Optional[str] = Field(default=None, description="Descrição (opcional).")
    payment_method: Optional[str] = Field(default=None, description="Forma de pagamento (opcional).")
    category_name: Optional[str] = Field(default=None, description="Nome da categoria (Alimentação).")
class QueryTransactionsArgs(BaseModel):
    text: Optional[str] = Field(default=None, description="Filtro por texto (source_text/description).")
    type_name: Optional[str] = Field(default=None, description="Nome do tipo da transação (INCOME | EXPENSES | TRANSFER).")
    date_local: Optional[str] = Field(default=None, description="Data local exata no formato YYYY-MM-DD (opcional).")
    date_from_local: Optional[str] = Field(default=None, description="Data inicial para filtro (YYYY-MM-DD).")
    date_to_local: Optional[str] = Field(default=None, description="Data final para filtro (YYYY-MM-DD).")
    limit: int = Field(default=20, description="Número máximo de resultados (default = 20).")



TYPE_ALIASES = {
    "INCOME":"INCOME", "ENTRADA":"INCOME", "RECEITA":"INCOME","SALÁRIO":"INCOME",
    "EXPENSE":"EXPENSES", "EXPENSES":"EXPENSES", "SAÍDA":"EXPENSES", "DESPESA":"EXPENSES",
    "TRANSFER":"TRANSFER", "TRANSFERÊNCIA":"TRANSFER"
}

def _get_category_id(cur, category_id: Optional[int], category_name: Optional[str]) -> Optional[int]:
    if category_id:
        return category_id
    if category_name:
        cur.execute("SELECT id FROM categories WHERE UPPER(name)=UPPER(%s) LIMIT 1;", (category_name.strip(),))
        row = cur.fetchone()
        return row[0] if row else None
    return None

def _resolve_type_id(cur, type_id: Optional[int], type_name: Optional[str]) -> Optional[int]:
    if type_name:
        t = type_name.strip().upper()
        t = TYPE_ALIASES.get(t, t) 
        cur.execute("SELECT id FROM transaction_types WHERE UPPER(type)=%s LIMIT 1;", (t,))
        row = cur.fetchone()
        return row[0] if row else None
    if type_id:
        return int(type_id)
    return 2 

@tool("add_transaction", args_schema=AddTransactionArgs)
def add_transaction(
    amount: float,
    source_text: str,
    occurred_at: Optional[str] = None,
    type_id: Optional[int] = None,
    type_name: Optional[str] = None,
    category_id: Optional[int] = None,
    category_name: Optional[str] = None, 
    description: Optional[str] = None,
    payment_method: Optional[str] = None,
) -> dict:
    """Insere uma transação financeira no banco de dados Postgres."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        resolved_type_id = _resolve_type_id(cur, type_id, type_name)
        resolved_category_id = _get_category_id(cur, category_id, category_name)

        if not resolved_type_id:
            return {"status": "error", "message": "Tipo inválido (use type_id ou type_name: INCOME/EXPENSES/TRANSFER)."}

        if occurred_at:
            cur.execute(
                """
                INSERT INTO transactions
                    (amount, "type", category_id, description, payment_method, occurred_at, source_text)
                VALUES
                    (%s, %s, %s, %s, %s, %s::timestamptz, %s)
                RETURNING id, occurred_at;
                """,
                (amount, resolved_type_id, resolved_category_id, description, payment_method, occurred_at, source_text),
            )
        else:
            cur.execute(
                """
                INSERT INTO transactions
                    (amount, "type", category_id, description, payment_method, occurred_at, source_text)
                VALUES
                    (%s, %s, %s, %s, %s, NOW(), %s)
                RETURNING id, occurred_at;
                """,
                (amount, resolved_type_id, resolved_category_id, description, payment_method, source_text),
            )

        new_id, occurred = cur.fetchone()
        conn.commit()
        return {"status": "ok", "id": new_id, "occurred_at": str(occurred)}

    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


class QueryTransactionsArgs(BaseModel):
    text: Optional[str] = None
    type_name: Optional[str] = None
    date_local: Optional[str] = None
    date_from_local: Optional[str] = None
    date_to_local: Optional[str] = None
    limit: int = 20


@tool("query_transactions", args_schema=QueryTransactionsArgs)
def query_transactions(
    text: Optional[str] = None,
    type_name: Optional[str] = None,
    date_local: Optional[str] = None,
    date_from_local: Optional[str] = None,
    date_to_local: Optional[str] = None,
    limit: int = 20,
) -> dict:
    """
    Consulta transações com filtros por texto (source_text/description),
    tipo e datas locais (America/Sao_Paulo).

    Os dados devem vir na seguinte ordem:
    - Intervalo (date_from_local/date_to_local): ASC (cronológico).
    - Caso contrário: DESC (mais recentes primeiro).
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        query = """
            SELECT
                t.id, t.amount, tt.type as type_name, c.name as category_name, t.description, t.payment_method, t.occurred_at, t.source_text
            FROM
                transactions t
            JOIN
                transaction_types tt ON t.type = tt.id
            LEFT JOIN
                categories c ON t.category_id = c.id
            WHERE 1=1
        """
        params = []

        if text:
            query += " AND (t.source_text ILIKE %s OR t.description ILIKE %s)"
            params.extend([f"%{text}%", f"%{text}%"])
        
        if type_name:
            resolved_type_id = _resolve_type_id(cur, None, type_name)
            if resolved_type_id:
                query += " AND t.type = %s"
                params.append(resolved_type_id)
            else:
                return {"status": "error", "message": f"Tipo de transação '{type_name}' inválido."}

        if date_local:
            query += " AND t.occurred_at::date = %s::date AT TIME ZONE 'America/Sao_Paulo'"
            params.append(date_local)
        elif date_from_local and date_to_local:
            query += " AND t.occurred_at::date BETWEEN %s::date AND %s::date AT TIME ZONE 'America/Sao_Paulo'"
            params.extend([date_from_local, date_to_local])
            query += " ORDER BY t.occurred_at ASC"
        else:
            query += " ORDER BY t.occurred_at DESC"

        query += " LIMIT %s"
        params.append(limit)

        cur.execute(query, params)
        transactions = cur.fetchall()
        
        col_names = [desc[0] for desc in cur.description]
        
        results = []
        for row in transactions:
            results.append(dict(zip(col_names, row)))

        return {"status": "ok", "transactions": results}

    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

@tool("total_balance")
def total_balance() -> dict:
    """
    Retorna o saldo total (INCOME - EXPENSES) em todo o histórico.
    Ignora TRANSFER.
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        query = """
            SELECT
                SUM(CASE WHEN tt.type = 'INCOME' THEN t.amount ELSE -t.amount END)
            FROM
                transactions t
            JOIN
                transaction_types tt ON t.type = tt.id
            WHERE
                tt.type IN ('INCOME', 'EXPENSES');
        """
        cur.execute(query)
        balance = cur.fetchone()[0]
        return {"status": "ok", "total_balance": float(balance) if balance is not None else 0.0}

    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

@tool("daily_balance")
def daily_balance(date_local: str) -> dict:
    """
    Retorna o saldo (INCOME - EXPENSES) do dia local informado (YYYY-MM-DD)
    em America/Sao_Paulo. Ignora TRANSFER (type=3).
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        query = """
            SELECT
                SUM(CASE WHEN tt.type = 'INCOME' THEN t.amount ELSE -t.amount END)
            FROM
                transactions t
            JOIN
                transaction_types tt ON t.type = tt.id
            WHERE
                tt.type IN ('INCOME', 'EXPENSES')
                AND t.occurred_at::date = %s::date AT TIME ZONE 'America/Sao_Paulo';
        """
        cur.execute(query, (date_local,))
        balance = cur.fetchone()[0]
        return {"status": "ok", "daily_balance": float(balance) if balance is not None else 0.0}

    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

TOOLS = [add_transaction, query_transactions, total_balance, daily_balance]
