import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { EntityManager, Repository } from 'typeorm';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { ListarVeiculoDto } from './dto/listar-veiculo.dto';
import { PlacaVeiculoDto } from './dto/placa-veiculo.dto';
import { VeiculoEntity } from './entities/veiculo.entity';

@Injectable()
export class VeiculoService {
  constructor(
    @InjectRepository(VeiculoEntity)
    private readonly veiculoRepository: Repository<VeiculoEntity>,
  ) {}

  async listarTodos(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (veiculoRepository) => {
      const veiculos = await veiculoRepository.find({
        where: { idEmpresa: String(idEmpresa) },
        order: { placa: 'ASC', idVeiculo: 'ASC' },
      });

      const dados = veiculos.map((veiculo) => this.mapearVeiculo(veiculo));

      return {
        sucesso: true,
        total: dados.length,
        veiculos: dados,
      };
    });
  }

  async listarPlacas(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (veiculoRepository) => {
      const veiculos = await veiculoRepository.find({
        where: { idEmpresa: String(idEmpresa) },
        order: { placa: 'ASC', idVeiculo: 'ASC' },
        select: {
          idVeiculo: true,
          placa: true,
          placa2: true,
          placa3: true,
          placa4: true,
        },
      });

      const placas = veiculos.flatMap((veiculo) =>
        this.extrairPlacasDoVeiculo(veiculo),
      );

      const unicas = new Map<string, PlacaVeiculoDto>();
      for (const item of placas) {
        const chave = `${item.idVeiculo}-${item.placa}-${item.origemCampo}`;
        if (!unicas.has(chave)) {
          unicas.set(chave, item);
        }
      }

      const dados = Array.from(unicas.values()).sort((a, b) =>
        a.placa.localeCompare(b.placa, 'pt-BR'),
      );

      return {
        sucesso: true,
        total: dados.length,
        placas: dados,
      };
    });
  }

  private async executarComRls<T>(
    idEmpresa: number,
    callback: (
      veiculoRepository: Repository<VeiculoEntity>,
      manager: EntityManager,
    ) => Promise<T>,
  ): Promise<T> {
    return this.veiculoRepository.manager.transaction(async (manager) => {
      await configurarContextoEmpresaRls(manager, idEmpresa);
      const veiculoRepository = manager.getRepository(VeiculoEntity);
      return callback(veiculoRepository, manager);
    });
  }

  private mapearVeiculo(veiculo: VeiculoEntity): ListarVeiculoDto {
    return {
      idVeiculo: veiculo.idVeiculo,
      placa: veiculo.placa.trim().toUpperCase(),
      placasAdicionais: [veiculo.placa2, veiculo.placa3, veiculo.placa4]
        .filter(
          (item): item is string =>
            typeof item === 'string' && item.trim().length > 0,
        )
        .map((item) => item.trim().toUpperCase()),
      idMotoristaAtual: veiculo.idMotoristaAtual,
      kmAtual: veiculo.kmAtual,
      anoFabricacao: veiculo.anoFabricacao,
      anoModelo: veiculo.anoModelo,
      dataVencimento: veiculo.dataVencimento,
    };
  }

  private extrairPlacasDoVeiculo(veiculo: VeiculoEntity): PlacaVeiculoDto[] {
    const placas = [
      { valor: veiculo.placa, campo: 'placa', tipo: 'principal' },
      { valor: veiculo.placa2, campo: 'placa2', tipo: 'adicional' },
      { valor: veiculo.placa3, campo: 'placa3', tipo: 'adicional' },
      { valor: veiculo.placa4, campo: 'placa4', tipo: 'adicional' },
    ];

    return placas
      .filter(
        (item): item is { valor: string; campo: string; tipo: string } =>
          typeof item.valor === 'string' && item.valor.trim().length > 0,
      )
      .map((item) => ({
        idVeiculo: veiculo.idVeiculo,
        placa: item.valor.trim().toUpperCase(),
        tipo: item.tipo as PlacaVeiculoDto['tipo'],
        origemCampo: item.campo as PlacaVeiculoDto['origemCampo'],
      }));
  }
}
