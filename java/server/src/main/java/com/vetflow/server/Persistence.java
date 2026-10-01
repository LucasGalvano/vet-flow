package com.vetflow.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Persistencia simples para o Java Server - Parte 1.
 *
 * Espelha exatamente o comportamento de python/server/persistence.py:
 * cada arquivo MessagePack contem uma LISTA de registros (logins ou
 * canais). A cada escrita: le a lista inteira, adiciona o registro,
 * regrava o arquivo inteiro.
 *
 * Esta classe e "burra" de proposito -- regras de negocio (ex.: checar
 * nome de canal duplicado) ficam em Server.java, nao aqui.
 *
 * Concorrencia: assume um UNICO processo, single-threaded, processando
 * uma requisicao REP por vez -- mesma premissa do lado Python. Se o
 * server for paralelizado depois, esta classe precisara de lock de
 * arquivo.
 */
public class Persistence {

    private static final ObjectMapper MAPPER = new ObjectMapper(new MessagePackFactory());

    // Caminho relativo ao diretorio de trabalho de onde o jar e executado
    // (normalmente java/server/, ao rodar `java -jar target/vetflow-server.jar`
    // a partir dali). Sobrescrevivel via variavel de ambiente em Server.java.
    public static final String DEFAULT_DATA_DIR = "data";
    public static final String DEFAULT_LOGINS_PATH = DEFAULT_DATA_DIR + File.separator + "logins.msgpack";
    public static final String DEFAULT_CHANNELS_PATH = DEFAULT_DATA_DIR + File.separator + "channels.msgpack";

    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> loadList(String path) {
        File file = new File(path);
        if (!file.exists()) {
            return new ArrayList<>();
        }

        try {
            byte[] raw = Files.readAllBytes(file.toPath());
            if (raw.length == 0) {
                return new ArrayList<>();
            }
            List<Map<String, Object>> data = MAPPER.readValue(raw, List.class);
            return data != null ? data : new ArrayList<>();
        } catch (IOException e) {
            // Arquivo corrompido/truncado: nao derruba o servidor, apenas
            // comeca com lista vazia. Mesma decisao conservadora do lado
            // Python (ver persistence.py).
            System.err.println("[PERSISTENCE] Falha ao ler '" + path + "': " + e.getMessage());
            return new ArrayList<>();
        }
    }

    public static void saveList(List<Map<String, Object>> items, String path) {
        try {
            Path parent = Paths.get(path).getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            byte[] packed = MAPPER.writeValueAsBytes(items);
            Files.write(Paths.get(path), packed);
        } catch (IOException e) {
            throw new RuntimeException("Falha ao gravar '" + path + "'", e);
        }
    }

    public static List<Map<String, Object>> appendItem(Map<String, Object> record, String path) {
        List<Map<String, Object>> items = loadList(path);
        items.add(record);
        saveList(items, path);
        return items;
    }
}