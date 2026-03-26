import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { EntityManager, Repository } from 'typeorm';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { ListarMotoristaDto } from './dto/listar-motorista.dto';
import { MotoristaEntity } from './entities/motorista.entity';

@Injectable()
export class MotoristasService {
  constructor(
    @InjectRepository(MotoristaEntity)
    private readonly motoristaRepository: Repository<MotoristaEntity>,
  ) {}

  async listarTodos(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (motoristaRepository) => {
      const motoristas = await motoristaRepository.find({
        where: { idEmpresa: String(idEmpresa) },
        order: { nome: 'ASC', idMotorista: 'ASC' },
      });

      const dados = motoristas.map((motorista) =>
        this.mapearMotorista(motorista),
      );

      return {
        sucesso: true,
        total: dados.length,
        motoristas: dados,
      };
    });
  }

  private async executarComRls<T>(
    idEmpresa: number,
    callback: (
      motoristaRepository: Repository<MotoristaEntity>,
      manager: EntityManager,
    ) => Promise<T>,
  ): Promise<T> {
    return this.motoristaRepository.manager.transaction(async (manager) => {
      await configurarContextoEmpresaRls(manager, idEmpresa);
      const motoristaRepository = manager.getRepository(MotoristaEntity);
      return callback(motoristaRepository, manager);
    });
  }

  private mapearMotorista(motorista: MotoristaEntity): ListarMotoristaDto {
    return {
      idMotorista: motorista.idMotorista,
      nome: motorista.nome,
      cpf: motorista.cpf,
      cnh: motorista.cnh,
      categoriaCnh: motorista.categoriaCnh,
      validadeCnh: motorista.validadeCnh,
      status: motorista.status,
    };
  }
}
