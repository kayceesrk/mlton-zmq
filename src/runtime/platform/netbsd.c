#include "platform.h"

#include "diskBack.unix.c"
#include "displayMem.proc.c"
#include "mmap-protect.c"
#include "nonwin.c"
#include "sysctl.c"
#include "use-mmap.c"

static void catcher (__attribute__ ((unused)) int signo,
                     __attribute__ ((unused)) siginfo_t* info,
                     void* context) {
        ucontext_t* ucp = (ucontext_t*)context;
        GC_handleSigProf ((code_pointer) ucp->uc_mcontext.__gregs[_REG_EIP]);
}

void GC_setSigProfHandler (struct sigaction *sa) {
        sa->sa_flags = SA_ONSTACK | SA_RESTART;
        sa->sa_sigaction = (void (*)(int, siginfo_t*, void*))catcher;
}
