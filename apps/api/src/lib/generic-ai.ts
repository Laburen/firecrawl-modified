import { createOpenAI } from '@ai-sdk/openai';
import { createOllama } from "ollama-ai-provider";
import { createAzure } from '@ai-sdk/azure';

// Determinar qué proveedor usar basado en las variables de entorno
const getModelAdapter = () => {
    // Prioridad 1: Azure AI Services (detectar por el dominio services.ai.azure.com)
    if (process.env.AZURE_BASE_URL && process.env.AZURE_BASE_URL.includes('services.ai.azure.com') && process.env.AZURE_API_KEY) {
        console.log('🔵 Using Azure AI Services with URL:', process.env.AZURE_BASE_URL);
        return createOpenAI({
            apiKey: process.env.AZURE_API_KEY,
            baseURL: process.env.AZURE_BASE_URL,
            headers: {
                'api-key': process.env.AZURE_API_KEY,
            },
        });
    }
    
    // Prioridad 2: Azure OpenAI (tradicional)
    if (process.env.AZURE_RESOURCE_NAME && process.env.AZURE_API_KEY) {
        console.log('🔵 Using Azure OpenAI provider with resource:', process.env.AZURE_RESOURCE_NAME);
        return createAzure({
            resourceName: process.env.AZURE_RESOURCE_NAME,
            apiKey: process.env.AZURE_API_KEY,
            apiVersion: process.env.AZURE_API_VERSION || '2024-10-01-preview',
            baseURL: process.env.AZURE_BASE_URL, // Opcional
        });
    }
    
    // Prioridad 3: Ollama
    if (process.env.OLLAMA_BASE_URL) {
        console.log('🟠 Using Ollama provider with URL:', process.env.OLLAMA_BASE_URL);
        return createOllama({
            baseURL: process.env.OLLAMA_BASE_URL,
        });
    }
    
    // Prioridad 4: OpenAI (fallback)
    console.log('🟢 Using OpenAI provider');
    return createOpenAI({
        apiKey: process.env.OPENAI_API_KEY,
        baseURL: process.env.OPENAI_BASE_URL,
    });
};

const modelAdapter = getModelAdapter();

export function getModel(name: string) {
    // Si hay un modelo específico configurado, usarlo
    if (process.env.MODEL_NAME) {
        console.log("🟢 Using MODEL_NAME override:", process.env.MODEL_NAME);
        return modelAdapter(process.env.MODEL_NAME);
    }
    
    // Si es Azure OpenAI, el nombre debe ser el deployment name
    if (process.env.AZURE_RESOURCE_NAME) {
        return modelAdapter(name); // En Azure, name es el deployment name
    }
    
    return modelAdapter(name);
}

export function getEmbeddingModel(name: string) {
    // Si hay un modelo de embedding específico configurado, usarlo
    if (process.env.MODEL_EMBEDDING_NAME) {
        return modelAdapter.embedding(process.env.MODEL_EMBEDDING_NAME);
    }
    
    // Si es Azure OpenAI, usar el embedding adapter
    if (process.env.AZURE_RESOURCE_NAME) {
        return modelAdapter.embedding(name); // En Azure, name es el deployment name
    }
    
    return modelAdapter.embedding(name);
}
