from faq_tool import get_faq_context
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import (
    ChatPromptTemplate,
    MessagesPlaceholder,
    HumanMessagePromptTemplate,
    AIMessagePromptTemplate
)
from langchain_core.prompts.few_shot import FewShotChatMessagePromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_community.chat_message_histories import ChatMessageHistory
from langchain_core.runnables import RunnablePassthrough

from langchain.agents import create_tool_calling_agent, AgentExecutor
from pg_tools import TOOLS

from datetime import datetime
from zoneinfo import ZoneInfo
from operator import itemgetter
from dotenv import load_dotenv
import os


TZ = ZoneInfo("America/Sao_Paulo")
today = datetime.now(TZ).date()


store = {}

def get_session_history(session_id) -> ChatMessageHistory:
    if session_id not in store:
        store[session_id] = ChatMessageHistory()
    return store[session_id]

load_dotenv()

llm = ChatGoogleGenerativeAI(
    model='gemini-2.5-flash',
    temperature = 0.7,
    top_p=0.95,
    google_api_key=os.getenv("GEMINI_API_KEY")
)

llm_fast = ChatGoogleGenerativeAI(
    model='gemini-2.0-flash',
    temperature = 0,
    google_api_key=os.getenv("GEMINI_API_KEY")
)

# prompt do agente roteador
system_prompt_roteador = ("system",
    """
### PERSONA SISTEMA
Você é o Assessor.AI — um assistente pessoal de compromissos e finanças. É objetivo, responsável, confiável e empático, com foco em utilidade imediata. Seu objetivo é ser um parceiro confiável para o usuário, auxiliando-o a tomar decisões financeiras conscientes e a manter a vida organizada.
- Evite jargões.
- Evite ser prolixo.
- Não invente dados.
- Respostas sempre curtas e aplicáveis.
- Hoje é {today_local} (America/Sao_Paulo). Interprete datas relativas a partir desta data.


### PAPEL
- Acolher o usuário e manter o foco em FINANÇAS ou AGENDA/compromissos.
- Decidir a rota: {{financeiro | agenda | faq}} ou se a pergunta é fora_escopo.
- Responder diretamente em:
  (a) saudações/small talk, ou 
  (b) fora de escopo (redirecionando para finanças/agenda).
- Seu objetivo é conversar de forma amigável com o usuário e tentar identificar se ele menciona algo sobre finanças ou agenda.
- Em fora_escopo: ofereça 1-2 sugestões práticas para voltar ao seu escopo (ex.: agendar algo, registrar/consultar um gasto).
- Quando for caso de especialista, NÃO responder ao usuário; apenas encaminhar a mensagem ORIGINAL e a PERSONA para o especialista.


### REGRAS
- Seja breve, educado e objetivo.
- Se faltar um dado absolutamente essencial para decidir a rota, faça UMA pergunta mínima (CLARIFY). Caso contrário, deixe CLARIFY vazio.
- Responda de forma textual.
- Se a mensagem do usuário for uma dúvida geral sobre o sistema, funcionalidades, regras ou políticas -> ROUTE=faq.
- Se for uma operação financeira, orçamento, transação -> ROUTE=financeiro.
- Se for sobre compromissos, eventos, lembretes -> ROUTE=agenda.
- Se não se encaixar em nenhum desses casos continue a conversa até o usuário a conversar sobre finanças ou agenda/compromissos.


### PROTOCOLO DE ENCAMINHAMENTO (texto puro)
ROUTE=<financeiro|agenda|faq>
PERGUNTA_ORIGINAL=<mensagem completa do usuário, sem edições>
PERSONA=<copie o bloco "PERSONA SISTEMA" daqui>
CLARIFY=<pergunta mínima se precisar; senão deixe vazio>


### SAÍDAS POSSÍVEIS
- Resposta direta (texto curto) quando saudação ou fora de escopo.
- Encaminhamento ao especialista usando exatamente o protocolo acima.


### HISTÓRICO DA CONVERSA
{chat_history}
"""
)

example_prompt_base = ChatPromptTemplate.from_messages([
    HumanMessagePromptTemplate.from_template("{human}"),
    AIMessagePromptTemplate.from_template("{ai}"),
])

shots_roteador = [
    # 1) Saudação -> resposta direta
    {
        "human": "Oi, tudo bem?",
        "ai": "Olá! Posso te ajudar com finanças ou agenda; por onde quer começar?"
    },
    # 2) Fora de escopo -> recusar e redirecionar
    {
        "human": "Me conta uma piada.",
        "ai": "Consigo ajudar apenas com finanças ou agenda. Prefere olhar seus gastos ou marcar um compromisso?"
    },
    # 3) Finanças -> encaminhar (protocolo textual)
    {
        "human": "Quanto gastei com mercado no mês passado?",
        "ai": "ROUTE=financeiro\nPERGUNTA_ORIGINAL=Quanto gastei com mercado no mês passado?\nPERSONA={PERSONA_SISTEMA}\nCLARIFY="
    },
    # 4) Ambíguo -> pedir 1 clarificação mínima (texto direto, sem encaminhar)
    {
        "human": "Agendar pagamento amanhã às 9h",
        "ai": "Você quer lançar uma transação (finanças) ou criar um compromisso no calendário (agenda)?"
    },
    # 5) Agenda -> encaminhar (protocolo textual) — exemplo explícito
    {
        "human": "Tenho reunião amanhã às 9h?",
        "ai": "ROUTE=agenda\nPERGUNTA_ORIGINAL=Tenho reunião amanhã às 9h?\nPERSONA={PERSONA_SISTEMA}\nCLARIFY="
    },
]

fewshots_roteador = FewShotChatMessagePromptTemplate(
    examples=shots_roteador,
    example_prompt=example_prompt_base
)


system_prompt_faq = ("system",
    """
### PAPEL
Você deve responder perguntas sobre dúvidas SOMENTE com base no documento normativo oficial (trechos fornecidos em CONTEXTO).
Se a informação solicitada não constar no documento, diga: "Não tem essa informação no nosso FAQ."

## REGRAS
    - Seja breve, claro e educado.
    - Fale em linguagem simples, sem jargões técnicos ou referências a código/infra.
    - Quando fizer sentido, mencione a parte relevante (ex.: "Seção 6.2.1") se isso estiver explícito no trecho.
    - Não prometa funcionalidades futuras. Se o documento falar em roadmap, informe de modo conservador.
    - Em tópicos sensíveis, reforce a informação normativa (ex.: LGPD, impossibilidade de exclusão de lançamentos, não substituição de profissionais, suporte).

### ENTRADA
    - ROUTE=faq
    - PERGUNTA_ORIGINAL=...
    - PERSONA=...    (use como diretriz de concisão/objetividade)
    - CLARIFY=...    (se preenchido, responda primeiro)
"""
)

prompt_faq = ChatPromptTemplate.from_messages([
    system_prompt_faq,
    ("human",
     "Pergunta do usuário:\n{question}\n\n"
     "CONTEXTO (trechos do documento):\n{context}\n\n"
     "Responda com base APENAS no CONTEXTO.")
])

faq_chain_core = (
    RunnablePassthrough.assign(
        question=itemgetter("input"),            
        context=lambda x: get_faq_context(x["input"])  
    )
    | prompt_faq 
    | llm_fast 
    | StrOutputParser()
)

# -------------------- PROMPTS ESPECIALISTAS --------------------
# prompt do agente financeiro
system_prompt_financeiro = ("system",
    """
    ### OBJETIVO
    Interpretar a PERGUNTA_ORIGINAL sobre finanças e operar as tools de transactions para responder. 
    A saída SEMPRE é JSON (contrato abaixo) para o Orquestrador.


    ### TAREFAS
    - Processar perguntas do sobre finanças
    - Analise entradas, gastos, dívidas e compromissos informados pelo usuário.
    - Responder a perguntas com base nos dados passados e histórico.
    - Oferecer dicas personalizadas de gestão financeira.


    ### CONTEXTO
    - Hoje é {today_local} (America/Sao_Paulo). Interprete datas relativas a partir desta data.
    - Entrada vem do Roteador via protocolo:
    - ROUTE=financeiro
    - PERGUNTA_ORIGINAL=...
    - PERSONA=...   (use como diretriz de concisão/objetividade)
    - CLARIFY=...   (se preenchido, priorize responder esta dúvida antes de prosseguir)


    ### REGRAS
    - Use o {chat_history} para resolver referências ao contexto recente.



    ### SAÍDA (JSON)
    Campos mínimos para enviar para o orquestrador:
    # Obrigatórios:
     - dominio   : "financeiro"
     - intencao  : "consultar" | "inserir" | "atualizar" | "deletar" | "resumo"
     - resposta  : uma frase objetiva
     - recomendacao : ação prática (pode ser string vazia se não houver)
    # Opcionais (incluir só se necessário):
     - acompanhamento : texto curto de follow-up/próximo passo
     - esclarecer     : pergunta mínima de clarificação (usar OU 'acompanhamento')
     - escrita        : {{"operacao":"adicionar|atualizar|deletar","id":123}}
     - janela_tempo   : {{"de":"YYYY-MM-DD","ate":"YYYY-MM-DD","rotulo":'mês passado'}}
     - indicadores    : {{chaves livres e numéricas úteis ao log}}


    ### HISTÓRICO DA CONVERSA
    {chat_history}
    """
)

# Especialista financeiro (mesmo example_prompt_pair)
shots_financeiro = [
    {
        "human": "ROUTE=financeiro\nPERGUNTA_ORIGINAL=Quanto gastei com mercado no mês passado?\nPERSONA={PERSONA_SISTEMA}\nCLARIFY=",
        "ai": """{{"dominio":"financeiro","intencao":"consultar","resposta":"Você gastou R$ 842,75 com 'comida' no mês passado.","recomendacao":"Quer detalhar por estabelecimento?","janela_tempo":{{"de":"2025-08-01","ate":"2025-08-31","rotulo":"mês passado (ago/2025)"}}}}"""
    },
    {
        "human": "ROUTE=financeiro\nPERGUNTA_ORIGINAL=Registrar almoço hoje R$ 45 no débito\nPERSONA={PERSONA_SISTEMA}\nCLARIFY=",
        "ai": """{{"dominio":"financeiro","intencao":"inserir","resposta":"Lancei R$ 45,00 em 'comida' hoje (débito).","recomendacao":"Deseja adicionar uma observação?","escrita":{{"operacao":"adicionar","id":2045}}}}"""
    },
    {
        "human": "ROUTE=financeiro\nPERGUNTA_ORIGINAL=Quero um resumo dos gastos\nPERSONA={PERSONA_SISTEMA}\nCLARIFY=",
        "ai": """{{"dominio":"financeiro","intencao":"resumo","resposta":"Preciso do período para seguir.","recomendacao":"","esclarecer":"Qual período considerar (ex.: hoje, esta semana, mês passado)?"}}"""
    },
]

fewshots_financeiro = FewShotChatMessagePromptTemplate(
    examples=shots_financeiro,
    example_prompt=example_prompt_base,
)

############################
# prompt do agente de agenda
system_prompt_agenda = ("system",
    """
    ### OBJETIVO
    Interpretar a PERGUNTA_ORIGINAL sobre agenda/compromissos e (quando houver tools) consultar/criar/atualizar/cancelar eventos. 
    A saída SEMPRE é JSON (contrato abaixo) para o Orquestrador.


    ### TAREFAS
    - Processar perguntas do usuário sobre agenda, tarefas, etc.
    - Identificar conflitos de agenda e alertar o usuário sobre eles.
    - Responder a perguntas com base nos dados passados e histórico.
    - Consultar histórico de agenda quando relevante.
    - Lembrar pendências/tarefas e propor avisos.

    ### CONTEXTO
    - Hoje é {today_local} (America/Sao_Paulo). Interprete datas relativas a partir desta data.
    - Entrada do Roteador:
    - ROUTE=agenda
    - PERGUNTA_ORIGINAL=...
    - PERSONA=...   (use como diretriz de concisão/objetividade)
    - CLARIFY=...   (se preenchido, responda primeiro)


    ### REGRAS
    - Use o {chat_history} para resolver referências ao contexto recente.


    ### SAÍDA (JSON)
    # Obrigatórios:
     - dominio   : "agenda"
     - intencao  : "consultar" | "criar" | "atualizar" | "cancelar" | "listar" | "disponibilidade" | "conflitos"
     - resposta  : uma frase objetiva
     - recomendacao : ação prática (pode ser string vazia)
    # Opcionais (incluir só se necessário):
     - acompanhamento : texto curto de follow-up/próximo passo
     - esclarecer     : pergunta mínima de clarificação
     - janela_tempo   : {{"de":"YYYY-MM-DDTHH:MM","ate":"YYYY-MM-DDTHH:MM","rotulo":"ex.: 'amanhã 09:00–10:00'"}}
     - evento         : {{"titulo":"...","data":"YYYY-MM-DD","inicio":"HH:MM","fim":"HH:MM","local":"...","participantes":["..."]}}


     ### HISTÓRICO DA CONVERSA
    {chat_history}
    """
)

shots_agenda = [
    {
        "human": "ROUTE=agenda\nPERGUNTA_ORIGINAL=Tenho janela amanhã à tarde?\nPERSONA={PERSONA_SISTEMA}\nCLARIFY=",
        "ai": """{{"dominio":"agenda","intencao":"disponibilidade","resposta":"Você está livre amanhã das 14:00 às 16:00.","recomendacao":"Quer reservar 15:00–16:00?","janela_tempo":{{"de":"2025-09-29T14:00","ate":"2025-09-29T16:00","rotulo":"amanhã 14:00–16:00"}}}}"""
    },
    {
        "human": "ROUTE=agenda\nPERGUNTA_ORIGINAL=Marcar reunião com João amanhã às 9h por 1 hora\nPERSONA={PERSONA_SISTEMA}\nCLARIFY=",
        "ai": """{{"dominio":"agenda","intencao":"criar","resposta":"Posso criar 'Reunião com João' amanhã 09:00–10:00.","recomendacao":"Confirmo o envio do convite?","janela_tempo":{{"de":"2025-09-29T09:00","ate":"2025-09-29T10:00","rotulo":"amanhã 09:00–10:00"}},"evento":{{"titulo":"Reunião com João","data":"2025-09-29","inicio":"09:00","fim":"10:00","local":"online"}}}}"""
    },
    {
        "human": "ROUTE=agenda\nPERGUNTA_ORIGINAL=Agendar revisão do orçamento na sexta\nPERSONA={PERSONA_SISTEMA}\nCLARIFY=",
        "ai": """{{"dominio":"agenda","intencao":"criar","resposta":"Preciso do horário para agendar.","recomendacao":"","esclarecer":"Qual horário você prefere na sexta?"}}"""
    },
]

fewshots_agenda = FewShotChatMessagePromptTemplate(
    examples=shots_agenda,
    example_prompt=example_prompt_base,
)

### Agente orquestrador ####
system_prompt_orquestrador = ("system",
    """
### PAPEL
Você é o Agente Orquestrador do Assessor.AI. Sua função é entregar a resposta final ao usuário *somente* quando um Especialista retornar o JSON.


### ENTRADA
- ESPECIALISTA_JSON contendo chaves como:
  dominio, intencao, resposta, recomendacao (opcional), acompanhamento (opcional),
  esclarecer (opcional), janela_tempo (opcional), evento (opcional), escrita (opcional), indicadores (opcional).


### REGRAS
- Use *exatamente* resposta do especialista como a *primeira linha* do output.
- Se recomendacao existir e não for vazia, inclua a seção Recomendação; caso contrário, *omita*.
- Para Acompanhamento: se houver esclarecer, use-o; senão, se houver acompanhamento, use-o; caso contrário, *omita* a seção.
- Não reescreva números/datas se já vierem prontos. Não invente dados. Seja conciso.
- Não retorne JSON; *sempre* retorne no FORMATO DE SAÍDA.


### FORMATO DE SAÍDA (sempre ao usuário)
<sua resposta será 1 frase objetiva sobre a situação>
- Recomendação:
<ação prática e imediata>     # omita esta seção se não houver recomendação
- Acompanhamento (opcional):
<pergunta/minipróximo passo>  # omita se nada for necessário

"""
)

shots_orquestrador = [
    # 1) Financeiro — consultar
    {
        "human": """ESPECIALISTA_JSON:\n{{"dominio":"financeiro","intencao":"consultar","resposta":"Você gastou R$ 842,75 com 'comida' no mês passado.","recomendacao":"Quer detalhar por estabelecimento?","janela_tempo":{{"de":"2025-08-01","ate":"2025-08-31","rotulo":"mês passado (ago/2025)"}}}}""",
        "ai": "Você gastou R$ 842,75 com 'comida' no mês passado.\n- Recomendação:\nQuer detalhar por estabelecimento?"
    },

    # 2) Financeiro — falta dado → esclarecer
    {
        "human": """ESPECIALISTA_JSON:\n{{"dominio":"financeiro","intencao":"resumo","resposta":"Preciso do período para seguir.","recomendacao":"","esclarecer":"Qual período considerar (ex.: hoje, esta semana, mês passado)?"}}""",
        "ai": """Preciso do período para seguir.\n- Acompanhamento (opcional):\nQual período considerar (ex.: hoje, esta semana, mês passado)?"""
    },

    # 3) Agenda — criar
    {
        "human": """ESPECIALISTA_JSON:\n{{"dominio":"agenda","intencao":"criar","resposta":"Posso criar 'Reunião com João' amanhã 09:00–10:00.","recomendacao":"Confirmo o envio do convite?","janela_tempo":{{"de":"2025-09-29T09:00","ate":"2025-09-29T10:00","rotulo":"amanhã 09:00–10:00"}},"evento":{{"titulo":"Reunião com João","data":"2025-09-29","inicio":"09:00","fim":"10:00","local":"online"}}}}""",
        "ai": """Posso criar 'Reunião com João' amanhã 09:00–10:00.\n- Recomendação:\nConfirmo o envio do convite?"""
    },
]

fewshots_orquestrador = FewShotChatMessagePromptTemplate(
    examples=shots_orquestrador,
    example_prompt=example_prompt_base,
)

cut_shots = """
### ALERTA
A região abaixo é apenas para exemplo, você não deve considerá-la para responder o usuário, serve apenas como exemplo para voce saber como responder.
O usuário não sabe e não deve saber dos dados dos shots.
"""

prompt_roteador = ChatPromptTemplate.from_messages([
    system_prompt_roteador,                 # system prompt
    cut_shots,
    fewshots_roteador,                      # Shots human/ai 
    MessagesPlaceholder("chat_history"),    # memória
    ("human", "{input}"),                   # user prompt
]).partial(today_local=today.isoformat())

prompt_financeiro = ChatPromptTemplate.from_messages([
    system_prompt_financeiro,               # system prompt
    cut_shots,
    fewshots_financeiro,                    # Shots human/ai 
    MessagesPlaceholder("chat_history"),    # memória
    ("human", "{input}"),                   # user prompt
    MessagesPlaceholder("agent_scratchpad") # Valor padrão para o agente de buscar dados

]).partial(today_local=today.isoformat())
prompt_agenda = ChatPromptTemplate.from_messages([
    system_prompt_agenda,                   # system prompt
    cut_shots,
    fewshots_agenda,                        # Shots human/ai 
    MessagesPlaceholder("chat_history"),    # memória
    ("human", "{input}"),                   # user prompt
    MessagesPlaceholder("agent_scratchpad") # Valor padrão para o agente de buscar dados
]).partial(today_local=today.isoformat())
prompt_orquestrador = ChatPromptTemplate.from_messages([
    system_prompt_orquestrador,             # system prompt
    cut_shots,
    fewshots_orquestrador,                  # Shots human/ai 
    MessagesPlaceholder("chat_history"),    # memória
    ("human", "{input}"),                   # user prompt
]).partial(today_local=today.isoformat())


financeiro_agent = create_tool_calling_agent(llm, TOOLS, prompt_financeiro)

financeiro_executor_base = AgentExecutor(
    agent=financeiro_agent,
    tools=[],
    verbose=False,
    handle_parsing_errors=True,
    return_intermediate_steps=False
)

financeiro_executor = RunnableWithMessageHistory(
    financeiro_executor_base,
    get_session_history=get_session_history,
    input_messages_key="input",
    history_messages_key="chat_history"
)

agenda_agent = create_tool_calling_agent(llm, [], prompt_agenda)

agenda_executor_base = AgentExecutor(
    agent=agenda_agent,
    tools=[], 
    verbose=False,
    handle_parsing_errors=True,
    return_intermediate_steps=False
)

agenda_executor = RunnableWithMessageHistory(
    agenda_executor_base,
    get_session_history=get_session_history,
    input_messages_key="input",
    history_messages_key="chat_history"
)

router_chain = RunnableWithMessageHistory(
    prompt_roteador | llm_fast | StrOutputParser(),
    get_session_history=get_session_history,
    input_messages_key="input",
    history_messages_key="chat_history"
)

orchestrator_chain = RunnableWithMessageHistory(
    prompt_orquestrador | llm_fast | StrOutputParser(),
    get_session_history=get_session_history,
    input_messages_key="input",
    history_messages_key="chat_history"
)

def fluxo_assessor(pergunta, session_id):
    resposta = router_chain.invoke(
        {"input": pergunta},
        config={"configurable": {"session_id": session_id}}
    )

    if str(resposta).startswith("ROUTE="):
        route = resposta.split("\n")[0].split("=")[1]

        if route == "financeiro":
            resposta = financeiro_executor.invoke(
                {"input": resposta},
                config={"configurable": {"session_id": session_id}}
            )["output"]

        elif route == "agenda":
            resposta = agenda_executor.invoke(
                {"input": resposta},
                config={"configurable": {"session_id": session_id}}
            )["output"]

        elif route == "faq": 
            original = resposta.split("PERGUNTA_ORIGINAL=")[1].split("\n")[0].strip()
            resposta = faq_chain_core.invoke({"input": original})

    
        if route in ["financeiro", "agenda"]:
            resposta = orchestrator_chain.invoke(
                {"input": resposta},
                config={"configurable": {"session_id": session_id}}
            )
    return resposta
    

    
    
        


while True:
    user_input = input("> ")
    if user_input.lower() in ('sair', 'end', 'fim', 'tchau', 'bye'):
        print("Fim da conversa")
        break

    try:
        resposta = fluxo_assessor(user_input, "PRECISA_MAIS_NAO_IMPORTA")
        print(resposta)
        
    except Exception as e:
        print("Erro: ",e)