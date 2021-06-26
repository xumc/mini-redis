package main

import (
	"unsafe"
)

type page struct {
	id       int
	flags    uint16
	count    uint16
	overflow uint32 // for pages with elePageFlag, overflow will be used only when create the first element
	ptr      uintptr
}

func (p *page) meta() *meta {
	return (*meta)(unsafe.Pointer(&p.ptr))
}

func (p *page) freelist() *freelist {
	return (*freelist)(unsafe.Pointer(&p.ptr))
}

func (p *page) elements() *Elements {
	return (*Elements)(unsafe.Pointer(&p.ptr))
}

func (p *page) usedSize() uint32 {
	es := p.elements()
	if p.count == 0 {
		return  0
	}
	lastEle := &es.eles[p.count-1]
	return uint32(uintptr(unsafe.Pointer(lastEle)) - uintptr(unsafe.Pointer(p))) + lastEle.pos + lastEle.kSize + lastEle.vSize
}