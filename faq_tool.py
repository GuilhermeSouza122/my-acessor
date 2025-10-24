import os
from langchain_community.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import FAISS
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from dotenv import load_dotenv

load_dotenv()

pdf_path = "FAQ_assessor_v1.1.pdf"

def get_faq_context(question: str) -> str:
    loader = PyPDFLoader(pdf_path)
    docs = loader.load()
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=700, chunk_overlap=150)
    chunks = text_splitter.split_documents(docs)
    embeddings = GoogleGenerativeAIEmbeddings(
        model="models/gemini-embedding-001",
        google_api_key=os.getenv("GEMINI_API_KEY"),
        transport='rest'
    )   

    db = FAISS.from_documents(chunks, embeddings)
    results = db.similarity_search(question, k=6)
    context = "\n".join([doc.page_content for doc in results])
    return context