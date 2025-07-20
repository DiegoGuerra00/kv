import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

class Cliente {
    private String[] servidorIPs;
    private int[] servidorPortas;
    private Map<String, Long> timestamps;
    private Random random;

    public Cliente() {
        this.servidorIPs = new String[3];
        this.servidorPortas = new int[3];
        this.timestamps = new HashMap<>();
        this.random = new Random();
    }

    public void inicializar() {
        Scanner scanner = new Scanner(System.in);

        System.out.println("=== INICIALIZAÇÃO DO CLIENTE ===");
        for (int i = 0; i < 3; i++) {
            System.out.print("Digite o IP do servidor " + (i + 1) + " (padrão: 127.0.0.1): ");
            String ip = scanner.nextLine().trim();
            if (ip.isEmpty()) {
                ip = "127.0.0.1";
            }
            servidorIPs[i] = ip;

            System.out.print("Digite a porta do servidor " + (i + 1) + " (padrão: " + (10097 + i) + "): ");
            String portaStr = scanner.nextLine().trim();
            if (portaStr.isEmpty()) {
                servidorPortas[i] = 10097 + i;
            } else {
                servidorPortas[i] = Integer.parseInt(portaStr);
            }
        }

        System.out.println("Cliente inicializado com sucesso!");
    }

    public void executarPUT() {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Digite a key: ");
        String key = scanner.nextLine();

        System.out.print("Digite o value: ");
        String value = scanner.nextLine();

        int servidorEscolhido = random.nextInt(3);
        String ip = servidorIPs[servidorEscolhido];
        int porta = servidorPortas[servidorEscolhido];

        try (Socket socket = new Socket(ip, porta);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            Mensagem putMsg = new Mensagem(Mensagem.TipoMensagem.PUT, key, value);
            out.writeObject(putMsg);

            Mensagem resposta = (Mensagem) in.readObject();

            if (resposta.getTipo() == Mensagem.TipoMensagem.PUT_OK) {
                timestamps.put(key, resposta.getTimestamp());
                System.out.println("PUT_OK key: " + key + " value " + value +
                        " timestamp " + resposta.getTimestamp() +
                        " realizada no servidor " + ip + ":" + porta);
            }

        } catch (Exception e) {
            System.err.println("Erro ao executar PUT: " + e.getMessage());
        }
    }

    public void executarGET() {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Digite a key: ");
        String key = scanner.nextLine();

        long timestampCliente = timestamps.getOrDefault(key, 0L);

        int servidorEscolhido = random.nextInt(3);
        String ip = servidorIPs[servidorEscolhido];
        int porta = servidorPortas[servidorEscolhido];

        try (Socket socket = new Socket(ip, porta);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            Mensagem getMsg = new Mensagem(Mensagem.TipoMensagem.GET, key, timestampCliente);
            out.writeObject(getMsg);

            Mensagem resposta = (Mensagem) in.readObject();

            if (resposta.getTipo() == Mensagem.TipoMensagem.WAIT_FOR_RESPONSE) {
                System.out.println("Aguardando resposta assíncrona...");

                // Aguardar resposta assíncrona
                ServerSocket serverSocket = new ServerSocket(0);
                int portaLocal = serverSocket.getLocalPort();

                // Informar ao servidor nossa porta para callback
                Mensagem callbackInfo = new Mensagem(Mensagem.TipoMensagem.GET, key, timestampCliente);
                callbackInfo.setClientePorta(portaLocal);
                out.writeObject(callbackInfo);

                Socket callbackSocket = serverSocket.accept();
                ObjectInputStream callbackIn = new ObjectInputStream(callbackSocket.getInputStream());
                Mensagem respostaAssincrona = (Mensagem) callbackIn.readObject();

                timestamps.put(key, respostaAssincrona.getTimestamp());
                System.out.println("GET key: " + key + " value: " + respostaAssincrona.getValue() +
                        " obtido do servidor " + ip + ":" + porta +
                        ", meu timestamp " + timestampCliente +
                        " e do servidor " + respostaAssincrona.getTimestamp());

                callbackSocket.close();
                serverSocket.close();
            } else {
                timestamps.put(key, resposta.getTimestamp());
                System.out.println("GET key: " + key + " value: " + resposta.getValue() +
                        " obtido do servidor " + ip + ":" + porta +
                        ", meu timestamp " + timestampCliente +
                        " e do servidor " + resposta.getTimestamp());
            }

        } catch (Exception e) {
            System.err.println("Erro ao executar GET: " + e.getMessage());
        }
    }

    public void executar() {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("\n=== MENU CLIENTE ===");
            System.out.println("1. INIT");
            System.out.println("2. PUT");
            System.out.println("3. GET");
            System.out.println("4. SAIR");
            System.out.print("Escolha uma opção: ");

            String opcao = scanner.nextLine();

            switch (opcao) {
                case "1":
                    inicializar();
                    break;
                case "2":
                    executarPUT();
                    break;
                case "3":
                    executarGET();
                    break;
                case "4":
                    System.out.println("Saindo...");
                    return;
                default:
                    System.out.println("Opção inválida!");
            }
        }
    }

    public static void main(String[] args) {
        Cliente cliente = new Cliente();
        cliente.executar();
    }
}