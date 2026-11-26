import {useId,useState,type ReactNode} from 'react';
import {Info} from 'lucide-react';

export default function FormulaTip({label,children}:{label:string;children:ReactNode}) {
  const id=useId();
  const [open,setOpen]=useState(false);
  return <span className="formula-tip"
    onPointerEnter={event=>{if(event.pointerType==='mouse') setOpen(true);}}
    onPointerLeave={()=>setOpen(false)} onBlur={()=>setOpen(false)}
    onKeyDown={event=>{if(event.key==='Escape') setOpen(false);}}>
    <button type="button" aria-label={label} aria-describedby={open?id:undefined}
      onFocus={()=>setOpen(true)} onClick={()=>setOpen(true)}><Info size={15}/></button>
    {open&&<span id={id} role="tooltip" className="estimate-popover">
      {children}
    </span>}
  </span>;
}

