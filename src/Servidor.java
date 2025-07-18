import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class Servidor {
    private String meuIP;
    private int minhaPorta;
    private String liderIP;
    private int liderPorta;
    private boolean souLider;
    private Map<String, String> tabelaHash;
    private Map<String, Long> timestamps;
    private long contadorTimestamp;
    private final Object lock = new Object();
    private ExecutorService threadPool;
    
    // Para controlar confirmações de replicação
    private Map<String, Integer> replicationConfirmations;
    private Map<String, Mensagem> pendingPutResponses;
    private Map<String, Socket> pendingClientSockets;
    private Map<String, ObjectOutputStream> pendingClientOutputs;
    
    public Servidor() {
        this.tabelaHash = new HashMap<>();
        this.timestamps = new HashMap<>();
        this.contadorTimestamp = 0;
        this.threadPool = Executors.newCachedThreadPool();
        this.replicationConfirmations = new HashMap<>();
        this.pendingPutResponses = new HashMap<>();
        this.pendingClientSockets = new HashMap<>();
        this.pendingClientOutputs = new HashMap<>();
    }
    
    public void inicializar() {
        Scanner scanner = new Scanner(System.in);
        
        System.out.print("Digite o IP deste servidor (padrão: 127.0.0.1): ");
        String ip = scanner.nextLine().trim();
        this.meuIP = ip.isEmpty() ? "127.0.0.1" : ip;
        
        System.out.print("Digite a porta deste servidor: ");
        this.minhaPorta = Integer.parseInt(scanner.nextLine());
        
        System.out.print("Digite o IP do líder (padrão: 127.0.0.1): ");
        String liderIPInput = scanner.nextLine().trim();
        this.liderIP = liderIPInput.isEmpty() ? "127.0.0.1" : liderIPInput;
        
        System.out.print("Digite a porta do líder: ");
        this.liderPorta = Integer.parseInt(scanner.nextLine());
        
        this.souLider = (this.meuIP.equals(this.liderIP) && this.minhaPorta == this.liderPorta);
        
        scanner.close();
        System.out.println("Servidor iniciado em " + meuIP + ":" + minhaPorta);
        System.out.println("Líder: " + liderIP + ":" + liderPorta);
        System.out.println("Sou líder: " + souLider);
    }
    
    public void iniciarServidor() {
        try (ServerSocket serverSocket = new ServerSocket(minhaPorta)) {
            System.out.println("Servidor ouvindo na porta " + minhaPorta);
            
            while (true) {
                Socket clientSocket = serverSocket.accept();
                threadPool.submit(new ClientHandler(clientSocket));
            }
        } catch (IOException e) {
            System.err.println("Erro no servidor: " + e.getMessage());
        }
    }
    
    private class ClientHandler implements Runnable {
        private Socket socket;
        
        public ClientHandler(Socket socket) {
            this.socket = socket;
        }
        
        @Override
        public void run() {
            try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                 ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
                
                Mensagem mensagem = (Mensagem) in.readObject();
                
                switch (mensagem.getTipo()) {
                    case PUT:
                        processarPUT(mensagem, out, socket);
                        break;
                    case GET:
                        processarGET(mensagem, out, socket);
                        break;
                    case REPLICATION:
                        processarREPLICATION(mensagem, out);
                        break;
                    case REPLICATION_OK:
                        processarREPLICATION_OK(mensagem);
                        break;
                }
                
            } catch (Exception e) {
                System.err.println("Erro ao processar cliente: " + e.getMessage());
            }
        }
    }
    
    private void processarPUT(Mensagem mensagem, ObjectOutputStream out, Socket socket) throws Exception {
        String clienteIP = socket.getInetAddress().getHostAddress();
        int clientePorta = socket.getPort();
        
        if (!souLider) {
            System.out.println("Encaminhando PUT key:" + mensagem.getKey() + " value:" + mensagem.getValue());
            
            // Encaminhar para o líder
            try (Socket liderSocket = new Socket(liderIP, liderPorta);
                 ObjectOutputStream liderOut = new ObjectOutputStream(liderSocket.getOutputStream());
                 ObjectInputStream liderIn = new ObjectInputStream(liderSocket.getInputStream())) {
                
                Mensagem putParaLider = new Mensagem(Mensagem.TipoMensagem.PUT, mensagem.getKey(), mensagem.getValue());
                putParaLider.setClienteIP(clienteIP);
                putParaLider.setClientePorta(clientePorta);
                liderOut.writeObject(putParaLider);
                
                Mensagem respostaLider = (Mensagem) liderIn.readObject();
                out.writeObject(respostaLider);
            }
        } else {
            System.out.println("Cliente " + clienteIP + ":" + clientePorta + " PUT key:" + mensagem.getKey() + " value:" + mensagem.getValue());
            
            synchronized (lock) {
                contadorTimestamp++;
                tabelaHash.put(mensagem.getKey(), mensagem.getValue());
                timestamps.put(mensagem.getKey(), contadorTimestamp);
                
                // Criar chave única para este PUT
                String putKey = mensagem.getKey() + "_" + contadorTimestamp;
                
                // Inicializar contador de confirmações
                replicationConfirmations.put(putKey, 0);
                
                // Preparar resposta para enviar após confirmações
                Mensagem putOk = new Mensagem(Mensagem.TipoMensagem.PUT_OK, mensagem.getKey(), mensagem.getValue(), contadorTimestamp);
                pendingPutResponses.put(putKey, putOk);
                pendingClientSockets.put(putKey, socket);
                pendingClientOutputs.put(putKey, out);
                
                // Replicar para outros servidores
                replicarParaOutrosServidores(mensagem.getKey(), mensagem.getValue(), contadorTimestamp, putKey);
            }
        }
    }
    
    private void processarGET(Mensagem mensagem, ObjectOutputStream out, Socket socket) throws Exception {
        String clienteIP = socket.getInetAddress().getHostAddress();
        int clientePorta = socket.getPort();
        
        synchronized (lock) {
            String key = mensagem.getKey();
            long timestampCliente = mensagem.getTimestamp();
            long timestampServidor = timestamps.getOrDefault(key, 0L);
            
            System.out.println("Cliente " + clienteIP + ":" + clientePorta + " GET key:" + key + " ts:" + timestampCliente + 
                             ". Meu ts é " + timestampServidor + ", portanto devolvendo " + 
                             (timestampServidor >= timestampCliente ? tabelaHash.getOrDefault(key, "NULL") : "WAIT_FOR_RESPONSE"));
            
            if (timestampServidor >= timestampCliente) {
                String value = tabelaHash.getOrDefault(key, "NULL");
                Mensagem resposta = new Mensagem(Mensagem.TipoMensagem.GET_RESPONSE, key, value, timestampServidor);
                out.writeObject(resposta);
            } else {
                Mensagem waitMsg = new Mensagem(Mensagem.TipoMensagem.WAIT_FOR_RESPONSE);
                out.writeObject(waitMsg);
                
                // Aguardar atualização assíncrona
                threadPool.submit(() -> {
                    try {
                        while (true) {
                            Thread.sleep(100);
                            synchronized (lock) {
                                long novoTimestamp = timestamps.getOrDefault(key, 0L);
                                if (novoTimestamp >= timestampCliente) {
                                    String value = tabelaHash.getOrDefault(key, "NULL");
                                    
                                    // Conectar de volta ao cliente
                                    try (Socket clientSocket = new Socket(clienteIP, mensagem.getClientePorta());
                                         ObjectOutputStream clientOut = new ObjectOutputStream(clientSocket.getOutputStream())) {
                                        
                                        Mensagem resposta = new Mensagem(Mensagem.TipoMensagem.GET_RESPONSE, key, value, novoTimestamp);
                                        clientOut.writeObject(resposta);
                                    }
                                    break;
                                }
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Erro na resposta assíncrona: " + e.getMessage());
                    }
                });
            }
        }
    }
    
    private void processarREPLICATION(Mensagem mensagem, ObjectOutputStream out) throws Exception {
        System.out.println("REPLICATION key:" + mensagem.getKey() + " value:" + mensagem.getValue() + " ts:" + mensagem.getTimestamp());
        
        synchronized (lock) {
            tabelaHash.put(mensagem.getKey(), mensagem.getValue());
            timestamps.put(mensagem.getKey(), mensagem.getTimestamp());
        }
        
        Mensagem replicationOk = new Mensagem(Mensagem.TipoMensagem.REPLICATION_OK);
        replicationOk.setKey(mensagem.getKey());
        replicationOk.setTimestamp(mensagem.getTimestamp());
        out.writeObject(replicationOk);
    }
    
    private void processarREPLICATION_OK(Mensagem mensagem) {
        if (souLider) {
            synchronized (lock) {
                // Criar chave única para identificar este PUT
                String putKey = mensagem.getKey() + "_" + mensagem.getTimestamp();
                
                // Incrementar contador de confirmações
                Integer confirmacoes = replicationConfirmations.get(putKey);
                if (confirmacoes != null) {
                    confirmacoes++;
                    replicationConfirmations.put(putKey, confirmacoes);
                    
                    // Verificar se recebeu confirmação de todos os servidores (2 servidores não-líder)
                    if (confirmacoes >= 2) {
                        // Enviar PUT_OK para o cliente
                        Mensagem putOk = pendingPutResponses.get(putKey);
                        Socket clientSocket = pendingClientSockets.get(putKey);
                        ObjectOutputStream clientOut = pendingClientOutputs.get(putKey);
                        
                        if (putOk != null && clientSocket != null && clientOut != null) {
                            try {
                                clientOut.writeObject(putOk);
                                
                                String clienteIP = clientSocket.getInetAddress().getHostAddress();
                                int clientePorta = clientSocket.getPort();
                                System.out.println("Enviando PUT_OK ao Cliente " + clienteIP + ":" + clientePorta + 
                                                 " da key:" + putOk.getKey() + " ts:" + putOk.getTimestamp());
                            } catch (Exception e) {
                                System.err.println("Erro ao enviar PUT_OK para cliente: " + e.getMessage());
                            }
                        }
                        
                        // Limpar estruturas pendentes
                        replicationConfirmations.remove(putKey);
                        pendingPutResponses.remove(putKey);
                        pendingClientSockets.remove(putKey);
                        pendingClientOutputs.remove(putKey);
                    }
                }
            }
        }
    }
    
    private void replicarParaOutrosServidores(String key, String value, long timestamp, String putKey) {
        // Portas padrão dos servidores
        int[] portas = {10097, 10098, 10099};
        
        for (int porta : portas) {
            if (porta != minhaPorta) {
                threadPool.submit(() -> {
                    try (Socket socket = new Socket("127.0.0.1", porta);
                         ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                         ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
                        
                        Mensagem replicationMsg = new Mensagem(Mensagem.TipoMensagem.REPLICATION, key, value, timestamp);
                        out.writeObject(replicationMsg);
                        
                        Mensagem resposta = (Mensagem) in.readObject();
                        
                        if (resposta.getTipo() == Mensagem.TipoMensagem.REPLICATION_OK) {
                            resposta.setKey(key);
                            resposta.setTimestamp(timestamp);
                            processarREPLICATION_OK(resposta);
                        }
                        
                    } catch (Exception e) {
                        System.err.println("Erro ao replicar para servidor na porta " + porta + ": " + e.getMessage());
                    }
                });
            }
        }
    }
    
    public void executar() {
        inicializar();
        iniciarServidor();
    }
    
    public static void main(String[] args) {
        Servidor servidor = new Servidor();
        servidor.executar();
    }
}