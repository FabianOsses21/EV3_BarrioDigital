import { TestBed } from '@angular/core/testing';
import { of,throwError,Subject } from 'rxjs';
import { BarrioService } from './barrio.service';
import { AuthService } from './auth.service';
import { OrderService } from './order.service';
import { Tramite } from '../models/barrio.models';
describe('Persistencia de BarrioService',()=>{
  let service:BarrioService;
  const row:Tramite={id:'server-id',tipoId:'luminarias',tipoNombre:'Luminarias',asunto:'Poste sin luz',descripcion:'Poste apagado en la calle',propietarioId:'user-1',propietarioNombre:'Vecino',estado:'INGRESADO',fechaCreacion:'2026-10-17T00:00:00',fechaActualizacion:'2026-10-17T00:00:00'};
  const api={getOrders:vi.fn(),getCatalog:vi.fn(),getQuotas:vi.fn(),createOrder:vi.fn(),editOrder:vi.fn(),deleteOrder:vi.fn(),updateOrderStatus:vi.fn()};
  beforeEach(()=>{
    vi.resetAllMocks();api.getOrders.mockReturnValue(of([]));api.getCatalog.mockReturnValue(of([]));api.getQuotas.mockReturnValue(of({}));
    TestBed.configureTestingModule({providers:[BarrioService,{provide:AuthService,useValue:{isLoggedIn:()=>true,userId:'user-1',puedeVerTodos:false}},{provide:OrderService,useValue:api}]});service=TestBed.inject(BarrioService);
  });
  it('no muestra un trámite antes de la confirmación HTTP',async()=>{
    const reply=new Subject<Tramite>();api.createOrder.mockReturnValue(reply);
    const promise=service.crearTramite('luminarias','Poste sin luz','Poste apagado','ok@example.test');
    expect(service.tramitesVisibles()).toEqual([]);reply.next(row);reply.complete();await promise;
    expect(service.tramitesVisibles()[0].id).toBe('server-id');
  });
  it('el error de guardado se muestra y no crea un registro ficticio',async()=>{
    api.createOrder.mockReturnValue(throwError(()=>new Error('Servidor caído')));
    await expect(service.crearTramite('luminarias','Poste sin luz','Poste apagado','ok@example.test')).rejects.toThrow('Servidor caído');
    expect(service.tramitesVisibles()).toEqual([]);expect(service.errorHttp()).toBe('Servidor caído');
  });
  it('acepta una lista vacía del servidor después de una eliminación',()=>{
    api.getOrders.mockReturnValue(of([row]));service.cargarDesdeBackend();expect(service.tramitesVisibles()).toHaveLength(1);
    api.getOrders.mockReturnValue(of([]));service.cargarDesdeBackend();expect(service.tramitesVisibles()).toEqual([]);
  });
  it('los cupos disponibles usan el conteo global del servidor, no los trámites visibles',()=>{
    const tipo={id:'luminarias',nombre:'Luminarias',descripcion:'',cupoDiario:5,activo:true};
    api.getCatalog.mockReturnValue(of([tipo]));api.getQuotas.mockReturnValue(of({luminarias:4}));
    api.getOrders.mockReturnValue(of([row]));
    service.cargarDesdeBackend();
    // El vecino solo ve su propio trámite, pero el cupo lo ocupan admisiones de todos.
    expect(service.cuposDisponibles('luminarias')).toBe(1);
  });
  it('una admisión local descuenta el cupo mostrado',async()=>{
    const tipo={id:'luminarias',nombre:'Luminarias',descripcion:'',cupoDiario:5,activo:true};
    api.getCatalog.mockReturnValue(of([tipo]));api.getQuotas.mockReturnValue(of({luminarias:3}));
    api.getOrders.mockReturnValue(of([row]));service.cargarDesdeBackend();
    const update=api.updateOrderStatus.mockReturnValue(of({...row,estado:'ADMITIDO'}));
    await service.cambiarEstado(row.id,'ADMITIDO');
    expect(update).toHaveBeenCalled();expect(service.cuposDisponibles('luminarias')).toBe(1);
  });
  it('una eliminación fallida conserva el trámite',async()=>{
    api.getOrders.mockReturnValue(of([row]));service.cargarDesdeBackend();api.deleteOrder.mockReturnValue(throwError(()=>new Error('Conflicto')));
    await expect(service.eliminarTramite(row.id)).rejects.toThrow();expect(service.tramitesVisibles()).toHaveLength(1);
  });
});
